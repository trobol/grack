require 'zlib'
require 'rack/request'
require 'rack/response'
require 'rack/utils'
require 'time'
require 'json'
require 'fileutils'


require 'grack/git'
require 'grack/lfs'


module Grack
  class Server
    attr_reader :git

    SERVICES = [
      ["POST", 'service_rpc',      "(.*?)/git-upload-pack$",  'upload-pack'],
      ["POST", 'service_rpc',      "(.*?)/git-receive-pack$", 'receive-pack'],

      ["POST", 'lfs_batch_request',"(.*?)/info/lfs/objects/batch$"],
      ["GET PUT",  'lfs_file',       "(.*?)/info/lfs/objects/([a-f0-9]{64})$"],
      
      ["GET",  'get_info_refs',    "(.*?)/info/refs$"],
      ["GET",  'get_text_file',    "(.*?)/HEAD$"],
      ["GET",  'get_text_file',    "(.*?)/objects/info/alternates$"],
      ["GET",  'get_text_file',    "(.*?)/objects/info/http-alternates$"],
      ["GET",  'get_info_packs',   "(.*?)/objects/info/packs$"],
      ["GET",  'get_text_file',    "(.*?)/objects/info/[^/]*$"],
      ["GET",  'get_loose_object', "(.*?)/objects/[0-9a-f]{2}/[0-9a-f]{38}$"],
      ["GET",  'get_pack_file',    "(.*?)/objects/pack/pack-[0-9a-f]{40}\\.pack$"],
      ["GET",  'get_idx_file',     "(.*?)/objects/pack/pack-[0-9a-f]{40}\\.idx$"],
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
      return render_not_found unless git.valid_repo?
      puts cmd
      puts @rpc
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

    def lfs_batch_request
      json_body = @req.body.read
      req_body = JSON.parse(json_body)

      operation = req_body["operation"]
      objects = req_body["objects"]

      # TODO - verify body


      objects_res = []
      base_url = @req.base_url + @req.script_name + @git.path + "/info/lfs/objects/"
      
      # TODO - validate oid and size exist and are valid
      objects.each do |obj|
        size = obj["size"]
        oid = obj["oid"]
        obj_res = obj.clone
        key_path = transform_key(oid)
        path = File.join(git.repo, "lfs/objects", key_path)
        url = base_url + oid
        exists = File.exist?(path)

        
        actions = {}
        error_code = 0

        if operation == "download"
          if exists
            actions["download"] = { 'href' => url }
            
          else
            error_code = 400
          end
        elsif operation == "upload"
          # if we already have the file
          # send no action
          if !exists
            actions["upload"] = { 'href' => url }
          end
        end

        if error_code != 0
          obj_res["error"] = {
            'code' => error_code,
            'message' => ""
          }
        elsif
          obj_res["actions"] = actions
        end

        objects_res.append obj_res
      end
      body = { "transfer" => "basic", "objects" => objects_res }.to_json
      [200, {"Content-Type" => 'application/vnd.git-lfs+json'}, [body]]
    end

    
    def lfs_obj_action(oid, size, url, authenticated)
      
      {
        'oid'           => oid,
        'size'          => size,
        'authenticated' => false,
        'actions'       => {
          'upload' => {
            'href'       => url
          },
        },
      }
    end
    # the client seems to be using something similar
    def transform_key(key)
      # the github server implmentation uses this but maybe short keys
      # should be rejected
	  if key.length < 5
		return key
	  end
      
	  File.join(key[0..1], key[2..3], key)
    end

    
    def lfs_file
      puts "lfs upload"
      return [400] unless match = Regexp.new("(.*?)/info/lfs/objects/([a-f0-9]{64})").match(@req.path_info)

      key = match[2]
     
      
      if @req.request_method == "GET"
        lfs_file_download(key)
      elsif  @req.request_method == "PUT"
        lfs_file_upload(key)
      end
    end

    def lfs_file_download(key)
      puts "downloading"
      key = transform_key(key)
      path = File.join("lfs/objects", key)
      send_file(path, "application/octet-stream") do end
    end

    def lfs_file_upload(key)
      # TODO make sure file doesnt exist
      puts "uploading"

      obj_folder = File.join(@git.repo, "lfs/objects", key[0..1], key[2..3])
      path = File.join(obj_folder, key)
      
      FileUtils.mkdir_p(obj_folder)
      File.write(path, @req.body.read)
      @res = Rack::Response.new
      @res.status = 200
      @res.finish
    end

    # ------------------------
    # logic helping functions
    # ------------------------

    # some of this borrowed from the Rack::File implementation
    def send_file(reqfile, content_type)
      reqfile = File.join(git.repo, reqfile)
      return render_not_found unless File.exists?(reqfile)

      return render_not_found unless reqfile == File.realpath(reqfile)

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
      #path = File.join(root, path)
      Grack::Git.new(@config[:git_path], path, root)
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

      SERVICES.each do |method, handler, match, rpc|
        next unless m = Regexp.new(match).match(@req.path_info)

        return ['not_allowed'] unless method.include? @req.request_method

        cmd = handler
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
        @req.body.read
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

    def render_not_found
      [404, PLAIN_TYPE, ["Not Found"]]
    end

    def render_no_access
      [403, PLAIN_TYPE, ["Forbidden"]]
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
