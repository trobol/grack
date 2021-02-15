require 'zlib'
require 'rack/request'
require 'rack/response'
require 'rack/utils'
require 'time'
require 'json'
require 'fileutils'
require 'digest'


require 'grack/git'


module Grack
  # FROM RAILS actionpack/lib/action_dispatch/http/response.rb
  # Avoid having to pass an open file handle as the response body.
  # Rack::Sendfile will usually intercept the response and uses
  # the path directly, so there is no reason to open the file.
  class FileBody #:nodoc:
    attr_reader :to_path

    def initialize(path)
      @to_path = path
    end

    def body
      File.binread(to_path)
    end

    # Stream the file's contents if Rack::Sendfile isn't present.
    def each
      File.open(to_path, "rb") do |file|
        while chunk = file.read(16384)
          yield chunk
        end
      end
    end
  end
  
  class Server
    attr_reader :git

    SERVICES = [
      [["POST"], ['service_rpc'],	"(.*?)/git-upload-pack$",  'upload-pack'],
      [["POST"], ['service_rpc'],	"(.*?)/git-receive-pack$", 'receive-pack'],

      [["GET"],  ['get_info_refs'],		"(.*?)/info/refs$"],
      [["GET"],  ['get_text_file'],		"(.*?)/HEAD$"],
      [["GET"],  ['get_text_file'],		"(.*?)/objects/info/alternates$"],
      [["GET"],  ['get_text_file'],		"(.*?)/objects/info/http-alternates$"],
      [["GET"],  ['get_info_packs'],	"(.*?)/objects/info/packs$"],
      [["GET"],  ['get_text_file'],		"(.*?)/objects/info/[^/]*$"],
      [["GET"],  ['get_loose_object'],	"(.*?)/objects/[0-9a-f]{2}/[0-9a-f]{38}$"],
      [["GET"],  ['get_pack_file'],		"(.*?)/objects/pack/pack-[0-9a-f]{40}\\.pack$"],
      [["GET"],  ['get_idx_file'],		"(.*?)/objects/pack/pack-[0-9a-f]{40}\\.idx$"],
      
      [["POST"], ['lfs_batch_request'],	"(.*?)/info/lfs/objects/batch$"],
      [["POST"], ['lfs_locks_verify'], 	"(.*?)/info/lfs/locks/verify$"],
      [["POST"], ['lfs_locks_delete'],  "(.*?)/info/lfs/locks/[a-f0-9]*/unlock$"],
      
      [["GET", "POST"],         ['lfs_locks_list', 'lfs_locks_create'],       "(.*?)/info/lfs/locks$"],
      [["GET", "POST", "PUT"],  ['lfs_download', 'lfs_verify', 'lfs_upload'], "(.*?)/info/lfs/objects/([a-f0-9]{64})$"],
    ]

    def initialize(config = false)
      set_config(config)
    end

    def set_config(config)
      @config = config || {}
    end

    def set_config_setting(key, value)
      @config[key] = value
    end

    def call(env)
      dup._call(env)
    end

    def _call(env)
      @env = env
      @req = Rack::Request.new(env)

      cmd, path, @reqfile, @rpc = match_routing

      return render_method_not_allowed if cmd == 'not_allowed'
      return render_not_found unless cmd

      @git = get_git(path)
      @relative_path = path
      return render_not_found unless git.valid_repo?
      self.method(cmd).call
    end

    # ---------------------------------
    # actual command handling functions
    # ---------------------------------

    # Uses chunked (streaming) transfer, otherwise response
    # blocks to calculate Content-Length header
    # http://en.wikipedia.org/wiki/Chunked_transfer_encoding

    CRLF = "\r\n"
    
    # potential improvement
    # 
    # this doesnt have to be chunked
    #
    def service_rpc
      return render_no_access unless has_access?(@rpc, true)

      input = read_body

      @res = Rack::Response.new
      @res.status = 200
      @res["Content-Type"] = "application/x-git-%s-result" % @rpc
      @res["Transfer-Encoding"] = "chunked"
      @res["Cache-Control"] = "no-cache"

      

      @res.finish do
        git.execute([@rpc, '--stateless-rpc', git.repo]) do |pipe|
          pipe.write(input)
          pipe.close_write

          chunk_count = 0
          while block = pipe.read(8192)     # 8KB at a time
            chunk = encode_chunk(block)
            @res.write chunk  # stream it to the client
          end

          @res.write terminating_chunk
        end
      end
    end

    def encode_chunk(chunk)
      size_in_hex = chunk.size.to_s(16)
      [size_in_hex, CRLF, chunk, CRLF].join
    end

    def terminating_chunk
      [0, CRLF, CRLF].join
    end

    def get_info_refs
      service_name = get_service_type
      return dumb_info_refs unless has_access?(service_name)

      refs = git.execute([service_name, '--stateless-rpc', '--advertise-refs', git.repo])

      @res = Rack::Response.new
      @res.status = 200
      @res["Content-Type"] = "application/x-git-%s-advertisement" % service_name
      hdr_nocache

      @res.write(pkt_write("# service=git-#{service_name}\n"))
      @res.write(pkt_flush)
      @res.write(refs)

      @res.finish
    end

    def dumb_info_refs
      git.update_server_info
      send_file(@reqfile, "text/plain; charset=utf-8") do
        hdr_nocache
      end
    end

    def get_info_packs
      # objects/info/packs
      send_file(@reqfile, "text/plain; charset=utf-8") do
        hdr_nocache
      end
    end

    def get_loose_object
      send_file(@reqfile, "application/x-git-loose-object") do
        hdr_cache_forever
      end
    end

    def get_pack_file
      send_file(@reqfile, "application/x-git-packed-objects") do
        hdr_cache_forever
      end
    end

    def get_idx_file
      send_file(@reqfile, "application/x-git-packed-objects-toc") do
        hdr_cache_forever
      end
    end

    def get_text_file
      send_file(@reqfile, "text/plain") do
        hdr_nocache
      end
    end

    # -----------------------
    # LFS
    # ----------------------
    LFS_CONTENT_TYPE = { "Content-Type" => 'application/vnd.git-lfs+json' }
    BATCH_MISSING = { :error => { :code => 404, :message => "" } }
    BATCH_INVALID = { :error => { :code => 422, :message => "" } }

    def lfs_batch_request
      req_body = JSON.parse(@req.body.read)

      operation = req_body["operation"]
      objects = req_body["objects"] 
      return [ 422, PLAIN_TYPE, [ "invalid operation" ]] unless objects.is_a?(Array) && ['download', 'upload'].include?(operation)
     

      base_url = File.join(@req.base_url, @req.script_name, @relative_path, "/info/lfs/objects/")

      objects_res = objects.map do |obj|
        size = obj["size"]
        oid = obj["oid"]
        return [422, LFS_CONTENT_TYPE, [ "invalid oid" ]] unless /[a-f0-9]{64}/.match(oid)
        
        path = File.join(git.repo, "lfs/objects", get_key_dir(oid), oid)
        url = File.join(base_url, oid)
        exists = File.exist?(path)
        
        o = case operation
            when "download"
              BATCH_MISSING unless exists
              BATCH_INVALID unless File.size(path) == size
              { :actions => { :download => { :href => url } } }
            when "upload"
              # if we already have the file1
              # send no action
              href = { :href => url }
              { :actions => { :upload => href, :verify => href} } unless exists
            end
        if o.nil?
          obj
        else
          obj.merge o
        end
      end
      
      [200, LFS_CONTENT_TYPE, [ { "transfer" => "basic", "objects" => objects_res }.to_json ]]
    end

    
    def lfs_download
      return render_bad_req unless key = lfs_get_key
      path = File.join(git.repo, "lfs/objects", get_key_dir(key), key)

      return render_not_found("File not found") unless File.file?(path) && File.readable?(path)

      headers = { "Content-Type" => "application/octet-stream", "Content-Length" => File.size?(path) }
      [200, headers, FileBody.new(path)]
    end

    def lfs_verify
      return render_bad_req unless key = lfs_get_key
      body = JSON.parse(@req.body.read)
      oid = body["oid"]
      size = body["size"]
      return render_bad_req unless oid == key
      
      path = File.join(git.repo, "lfs/objects", get_key_dir(key), key)
      return render_not_found unless File.exists?(path) && size == File.size?(path)
      
      [200, PLAIN_TYPE, []]
    end
    
    def lfs_upload
      return render_bad_req unless key = lfs_get_key

      obj_dir = File.join(git.repo, "lfs/objects", get_key_dir(key))
      path = File.join(obj_dir, key)

      return render_bad_req unless content_length = @req.content_length.to_i
      return render_conflict if File.exist?(path)
      
      FileUtils.mkdir_p(obj_dir)
      bytes_written = IO.copy_stream(@req.body, path, content_length)
      return render_bad_req if bytes_written < content_length
      
      [201, PLAIN_TYPE, []]
    end
  
    # -----------
    # LFS utils
    # -----------
    
    # the client seems to be using something similar
    def get_key_dir(key)
	  File.join(key[0..1], key[2..3])
    end

    def lfs_get_key
      return nil unless match = Regexp.new("(.*?)/info/lfs/objects/([a-f0-9]{64})").match(@req.path_info)
      key = match[2]
      # the github server implementation allows shorter keys
      # but the spec says that currently only sha256 hashes are supported
      return nil unless key.length == 64
      key
    end
    

    def lfs_error_file_size
      [400, PLAIN_TYPE, ["DATA SIZE RECIVED DID NOT MATCH CONTENT LENGTH"]]
    end
    
    # ------------------
    # LFS locking
    # ------------------
    
    def lfs_locks_create
      
      body = read_json_body
      return render_bad_req unless path = body["path"]
      
      locks = lfs_load_locks
      found_lock = locks.find { |lock| lock["path"] == path }
      return render_lock_exists(found_lock) if found_lock
      
      id = Digest::SHA256.hexdigest(path)
      locked_at = Time.now.to_datetime.rfc3339
      username = get_username
      lock = { "id" => id, "locked_at" => locked_at, "path" => path, "owner" => { "name" => username } }
      locks.append(lock)

      lfs_save_locks(locks)
     
      [200, LFS_CONTENT_TYPE, [ JSON.generate({ "lock" => lock }) ]]
    end
    
    def lfs_locks_delete
      body = read_json_body
      force = body["force"] # optional
      return render_bad_req unless match = @req.path_info.match(/([0-9a-f]{64})\/unlock$/)
      id = match[1]

      locks = lfs_load_locks
      lock_index = locks.find_index { |lock| lock["id"] == id }
      # the spec doesn't mention a 404 but it seems dumb to omit it
      return render_lock_not_found unless lock_index

      lock = locks[lock_index]
      username = get_username

      # user also needs push acces but the auth middleware should deal with that
      return render_lock_no_force unless force || lock["owner"]["name"] == username

      locks.delete_at(lock_index)
      lfs_save_locks(locks)
      
      
      [200, LFS_CONTENT_TYPE, [ JSON.generate({ "lock" => lock }) ]]
    end

    
    def lfs_locks_list
      path = @req.params["path"]
      id = @req.params["id"]
      cursor = @req.params["cursor"] || 0
      limit = @req.params["limit"] || 100
      
      locks = lfs_load_locks
      if path
        locks = locks.select { |lock| lock["path"] == path }
      end
      if id
        locks = locks.select { |lock| lock["id"] == id }
      end

      has_remaining = locks.length > ((cursor + 1) * limit)
      locks = paginate(locks, limit, cursor)
      
      
      msg = { "locks" => locks }
      msg["next_cursor"] = cursor + 1 if has_remaining
      
      [200, LFS_CONTENT_TYPE, [ JSON.generate(msg) ]]
    end

    def lfs_locks_verify
      body = read_body
      limit = body["limit"] || 100
      cursor = body["cursor"] || 0

      locks = lfs_load_locks
      has_remaining = locks.length > ((cursor+1) * limit)
      locks = paginate(locks, limit,  cursor)
   
      
      username = get_username
      ours = []
      theirs = []
      locks.each do |lock|
        if lock["owner"]["name"] == username
          ours.append(lock)
        else
          theirs.append(lock)
        end
      end

      msg = { "ours" => ours, "theirs" => theirs }
      
      msg["next_cursor"] = cursor + 1 if has_remaining
      
      [200, LFS_CONTENT_TYPE, [ JSON.generate(msg) ]]
    end
    


    # ------------------
    # LFS locking utils
    # ------------------

    
    def get_username
     @env['REMOTE_USER'] ? @env['REMOTE_USER'] : "unknown"
    end
    
    def lfs_load_locks
      path = File.join(git.repo, "lfs/locks.json")
      return [] unless File.exist?(path)
      
      data = File.read(path)
      JSON.parse(data)
    end

    def lfs_save_locks(locks)
      path = File.join(git.repo, "lfs/locks.json")
      data = JSON.generate(locks)
      File.write(path, data)
    end

    def read_json_body
      raw_body = @req.body.read
      JSON.parse(raw_body)
    end
    
    def paginate(arr, limit, cursor)
      start = limit * cursor
      arr.slice(start, limit)
    end
    
    def render_lock_exists(lock)
      msg = { "lock" => lock, "message" => "already created a lock" }
      [409, LFS_CONTENT_TYPE, [ JSON.generate(msg) ]]
    end

    def render_lock_not_found
      [404, LFS_CONTENT_TYPE, [ '{"message": "Not Found"}' ]]
    end

    def render_lock_no_force
      [403, LFS_CONTENT_TYPE, [ '{"message": "That lock belongs to another user"}' ]]
    end

    # ------------------------
    # logic helping functions
    # ------------------------

    # some of this borrowed from the Rack::File implementation
    def send_file(relative_path, content_type)
      reqfile = File.join(git.repo, relative_path)
      return render_not_found("File not found") unless File.exist?(reqfile)
      
      return render_not_found unless reqfile == File.absolute_path(relative_path, git.repo)

      # reqfile looks legit: no path traversal, no leading '|'

      @res = Rack::Response.new
      @res.status = 200
      @res["Content-Type"]  = content_type
      @res["Last-Modified"] = File.mtime(reqfile).httpdate

      yield

      if size = File.size?(reqfile)
        @res["Content-Length"] = size.to_s
        @res.finish do
          File.open(reqfile, "rb") do |file|
            while part = file.read(8192)
              @res.write part
            end
          end
        end
      else
        body = [File.read(reqfile)]
        size = Rack::Utils.bytesize(body.first)
        @res["Content-Length"] = size
        @res.write body
        @res.finish
      end
    end

    def get_git(path)
      root = @config[:project_root] || Dir.pwd
      path = File.join(root, path)
      Grack::Git.new(@config[:git_path], path)
    end

    def get_service_type
      service_type = @req.params['service']
      return false unless service_type
      return false if service_type[0, 4] != 'git-'
      service_type.gsub('git-', '')
    end

    def match_routing
      cmd = nil
      path = nil

      SERVICES.each do |methods, handlers, match, rpc|
        next unless m = Regexp.new(match).match(@req.path_info)

        handler_index = methods.index(@req.request_method)
        return ['not_allowed'] unless handler_index != nil

       
        cmd = handlers[handler_index]
        path = m[1]
        file = @req.path_info.sub(path + '/', '')

        return [cmd, path, file, rpc]
      end
      
      nil
    end

    def has_access?(rpc, check_content_type = false)
      if check_content_type
        conten_type = "application/x-git-%s-request" % rpc
        return false unless @req.content_type == conten_type
      end

      return false unless ['upload-pack', 'receive-pack'].include?(rpc)

      if rpc == 'receive-pack'
        return @config[:receive_pack] if @config.include?(:receive_pack)
      end

      if rpc == 'upload-pack'
        return @config[:upload_pack] if @config.include?(:upload_pack)
      end

      git.config_setting(rpc)
    end

    def read_body
      if @env["HTTP_CONTENT_ENCODING"] =~ /gzip/
        Zlib::GzipReader.new(@req.body).read
      else
        @req.body.read # TODO, stream file in
      end
    end

    

    # --------------------------------------
    # HTTP error response handling functions
    # --------------------------------------

    PLAIN_TYPE = { "Content-Type" => "text/plain" }

    def render_method_not_allowed
      if @env['SERVER_PROTOCOL'] == "HTTP/1.1"
        [405, PLAIN_TYPE, ["Method Not Allowed"]]
      else
        [400, PLAIN_TYPE, ["Bad Request"]]
      end
    end

    def render_bad_req
       [400, PLAIN_TYPE, ["Bad Request"]]
    end
    
    def render_not_found(msg = "Not Found")
      [404, PLAIN_TYPE, [msg]]
    end

    def render_no_access
      [403, PLAIN_TYPE, ["Forbidden"]]
    end

    def render_no_storage
      [507,  PLAIN_TYPE, ["Insufficient Storage"]]
    end

    def render_conflict
      [409, PLAIN_TYPE, ["Resource already exists"]]
    end
    # ------------------------------
    # packet-line handling functions
    # ------------------------------

    def pkt_flush
      '0000'
    end

    def pkt_write(str)
      (str.size + 4).to_s(16).rjust(4, '0') + str
    end

    # ------------------------
    # header writing functions
    # ------------------------

    def hdr_nocache
      @res["Expires"] = "Fri, 01 Jan 1980 00:00:00 GMT"
      @res["Pragma"] = "no-cache"
      @res["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
    end

    def hdr_cache_forever
      now = Time.now().to_i
      @res["Date"] = now.to_s
      @res["Expires"] = (now + 31536000).to_s;
      @res["Cache-Control"] = "public, max-age=31536000";
    end
  end
end
