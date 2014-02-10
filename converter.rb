#!/usr/bin/env ruby

require 'net/ftp'
require 'yaml'
require 'fileutils'
require 'find'

def progress_bar(percentage)
  bar = '['
  if percentage > 0
    percentage = 100 if percentage > 100
    bar += ('=' * (percentage-1)) + '>>' + (' ' * (100-percentage)) + ']'
  else
    bar += '>>' + ' ' * 99 + ']'
  end
  "\r#{bar} #{percentage}%" 
end

def execute_ffmpeg(command)
  STDOUT.sync = true
  command = "#{command} 2>&1"
  progress = nil
  exit_status = 0
  duration, duration_seconds, ellapsed_time, ellapsed_seconds, percentage = nil
  last_line = nil
  IO.popen(command) do |pipe|
    pipe.each("\r") do |line|
      if line =~ /Duration:(\s.?(\d*):(\d*):(\d*\.\d*))/
        duration = $2.to_s + ":" + $3.to_s + ":" + $4.to_s
        duration_seconds = ($2.to_f * 60 * 60) + ($3.to_f * 60) + ($4.to_f + $5.to_f / 100)
      end
      if line =~ /^frame=.*time=(\d{2}):(\d{2}):(\d{2}).(\d{2})/
        ellapsed_time = $1.to_s + ':' + $2.to_s + ':' + $3.to_s + '.' + $4.to_s
        ellapsed_seconds = ($1.to_f * 60 * 60) + ($2.to_f * 60) + $3.to_f + ($4.to_f / 100)
        #puts "#{ellapsed_time} (#{ellapsed_seconds})"
        percentage = ((ellapsed_seconds / duration_seconds) * 100).round
        print progress_bar(percentage)
      end
      last_line = line #it will be use to output an error in case of any failure
    end
  end
  raise last_line if $?.exitstatus != 0
  puts  
end

def get_video_resolution(file_name)
  command = "ffprobe -show_streams \"#{file_name}\" 2>&1"
  width = nil
  height = nil
  IO.popen(command) do |pipe|
    pipe.each("\r") do |line|
      if line =~ /width=(\d+)/
        width = $1.to_i
      end
      if line =~ /height=(\d+)/
        height = $1.to_i
      end
    end
  end
  "#{width}x#{height}"
end

def get_video_duration(file_name)
  command = "ffprobe -show_streams \"#{file_name}\" 2>&1 | grep duration"
  duration = nil
  IO.popen(command) do |pipe|
    pipe.each("\n") do |line|
      if line =~ /duration=(\d+\.\d+)/
        if duration.nil?
          duration = $1.to_f
        elsif $1.to_f > duration
          duration = $1.to_f
        end
      end
    end
  end
  duration
end


def get_meta_data(file_name)
  command = "ffprobe -show_streams \"#{file_name}\" 2>&1"
  width = nil
  height = nil
  IO.popen(command) do |pipe|
    pipe.each("\r") do |line|
      if line =~ /width=(\d+)/
        width = $1.to_i
      end
      if line =~ /height=(\d+)/
        height = $1.to_i
      end
    end
  end
  { :width => width, :height => height }
end

def get_files_list(dir_name, extension)
  #Dir.chdir(dir_name)
  #Dir.glob("*.#{extension}")
  Dir.glob(dir_name + "/**/*.#{extension}")
end

def recreate_dir_structure(src, dst, extension)
  FileUtils.mkdir dst unless File.exists? dst
  FileUtils.mkdir (dst + '/' + File.basename(src)) unless File.exists? (dst + '/' + File.basename(src))
  dir_structure = []
  Find.find(src) do |path|
    if FileTest.directory? path
      if Dir.glob(path + "/**/*.#{extension}").empty?
        Find.prune
      else
        struct_path = dst + '/' + File.basename(src) + '/' + path.slice(src.size..path.size)
        unless File.exists? struct_path
          FileUtils.mkdir struct_path
        end
      end
    end
  end
end



def get_rates(resolution, bitrate_ratio)
  
  if resolution =~ /(\d+)x(\d+)/
    width = $1.to_i
    height = $2.to_i
    space = width*height
    bitrate = (space.to_f * (bitrate_ratio.to_f * 1e-6)).round(1)
    maxrate = (bitrate.to_f * 1.2).round(1)
    [ maxrate, bitrate ]
  else
    puts "[-] Error. Wrong resolution: #{resolution}"
    [ -1, -1 ]
  end

end

def get_meta_data(file_path)
  command = "ffprobe -show_streams \"#{file_path}\" 2>&1"
  width = nil
  height = nil
  counter = 0
  audio_presence = 'no audio'
  video_bitrate = 'unknown'
  file_size = File.size(file_path)
  stream_begins = false
  streams_data = []
  stream_index = 0
  IO.popen(command) do |pipe|
    pipe.each("\n") do |line|
      line.chomp!
      if line =~ /\[STREAM\]/ 
        stream_begins = true
        streams_data[stream_index] = Hash.new
      end
      if line =~ /\[\/STREAM\]/
        stream_begins = false 
        stream_index += 1
      end

      if stream_begins
        if line =~ /(.*)=(.*)/
          streams_data[stream_index][$1] = $2
        end
      end
    end
  end
  
  streams_data.each do |stream|
    if stream['codec_type'] == 'audio'
      audio_presence = 'with audio'
    end
    if stream['codec_type'] == 'video'
       video_bitrate = (stream['bit_rate'].to_f / 1024).round.to_s + ' kb/s'
    end
  end

  [ file_path, (File.size(file_path).to_f / 2**20).round(2).to_s + 'MB', video_bitrate, audio_presence ]

end

def safe_execute
  yield
rescue Exception => e
  puts "*************************Error during convertion****************************"
  puts e.message
  puts e.backtrace.inspect
  puts "****************************************************************************"
  exit 1
end


def ftp_upload(server_address, src, dst)
  STDOUT.sync = true
  ftp = Net::FTP.new(server_address)
  ftp.login

  dirs = dst.split('/') 
  dirs_size = dirs.size
  dirs = dirs[0..dirs_size-2]
  dirs.each do |leaf|
    ftp.mkdir(leaf) unless ftp.list(ftp.pwd).any? { |dir| dir.split.last.match(/^#{leaf}$/) }
    ftp.chdir(leaf)
  end

  count = 0
  percentage = 0
  file_size = File.size(src)
  ftp.putbinaryfile(src, File.basename(src), 1024) do |block|
    count += 1024.0
    percentage = (count / file_size * 100).round
    print progress_bar(percentage)
  end
  puts
end

def mp4box_processing(file_name)
  STDOUT.sync = true
  command = "MP4Box -inter 500 \"#{file_name}\" 2>&1"
  IO.popen(command) do |pipe|
    pipe.each("\r") do |line|
      if line =~ /^ISO File Writing.*\((\d+)\/(\d+)\).*$/
        percentage = ($1.to_f * 100.0 / $2.to_f).ceil
        percentage = 100 if percentage == 99
        print progress_bar(percentage)
      end
    end
  end
  puts
end

def cut_video(src, dst, start_t, end_t)

  command = "ffmpeg -i \"#{src}\" -y -ss #{start_t} -to #{end_t} \"#{dst}\""
  execute_ffmpeg(command)

end


unless File.exists?('config.yml')
  puts "\n[-] Can't find configuration file (config.yml)\n\n"
  exit
else
  config = YAML.load(File.read('config.yml'))
  %w(local_directory extension ftp_server upload_directory bitrate_ratio).each do |required_param|
    unless config.has_key?(required_param)
      puts "[-] Can't find '#{required_param}' in config.yml"
      exit
    end
  end
  available_params = %(path resolution bitrate audio exclude cut_start cut_end)
  if config.has_key?('files')
    special_files = {}
    config['files'].each do |special_settings|

      special_settings.keys.each do |param|
        unless available_params.include? param
          puts "[-] Error: Unrecognized setting '#{param}'"
          exit
        end
      end

      unless special_settings.has_key? 'path'
        puts "[-] Error: each 'file' record mush have a path: '#{special_settings}'."
        exit
      end

      unless File.exist?(config['local_directory'] + '/' + special_settings['path'])
        puts "[-] Error: File '#{config['local_directory']}/#{special_settings['path']}' does not exist!"
        exit
      end

      unless File.extname(special_settings['path']).eql? ('.' + config['extension'])
         puts "[-] Error: Extension of '#{config['local_directory']}/#{special_settings['path']}' is not equal to '#{config['extension']}'"
         exit 
      end

      if special_settings.has_key? 'resolution'
        resolution = special_settings['resolution']
        unless resolution =~ /^(\d+)x(\d+)$/
          puts "[-] Error: Wrong resolution: '#{resolution}' for #{special_settings['path']}"
          exit
        end
        if $1.to_i < 0 or $2.to_i < 0
          puts "[-] Error: Negative resolution: '#{resolution}' for '#{special_settings['path']}'"
          exit
        end
      end

      if special_settings.has_key? 'cut_start' and not special_settings.has_key? 'cut_end'
        puts "[-] Error: 'cut_start' must be closed with 'cut_end' param. '#{special_settings['path']}'"
        exit
      end

      if not special_settings.has_key? 'cut_start' and special_settings.has_key? 'cut_end'
        puts "[-] Error: 'cut_end' exists but 'cut_start' not found. '#{special_settings['path']}'"
        exit
      end

      if special_settings.has_key? 'cut_start' and special_settings.has_key? 'cut_end'
        cut_start =  special_settings['cut_start']
        cut_end = special_settings['cut_end']
        if cut_start > cut_end 
          puts "[-] Error: 'cut_start' is greater than 'cut_end'. '#{special_settings['path']}'"
          exit
        end
        video_duration = get_video_duration(config['local_directory'] + '/' + special_settings['path'])
        if cut_start > video_duration or cut_end > video_duration
          puts "[-] Error: Video duration (#{video_duration} seconds) less than 'cut_start' (#{cut_start}) or 'cut_end' (#{cut_end}) param. '#{special_settings['path']}'"
          exit
        end
      end


      if special_settings.keys.size < 2
        puts "[-] Error: each 'file' record mush have a path and additional argument: '#{special_settings}'."
        exit
      end

      special_files[config['local_directory'] + '/' + special_settings['path']] = special_settings

    end
  end
end

ffmpeg_command_main_tmpl = "ffmpeg -y -i \"%{filename}\" -acodec copy -bsf:a aac_adtstoasc -vcodec libx264  -vprofile main -level 31 -maxrate %{maxrate}M -minrate 100k -bufsize 10000000 -s %{resolution} -g 50 -vf yadif  -r 25  -b:v %{bitrate}M -f mp4"
ffmpeg_command_main_tmpl_without_audio = "ffmpeg -y -i \"%{filename}\" -vcodec libx264  -vprofile main -level 31 -maxrate %{maxrate}M -minrate 100k -bufsize 10000000 -s %{resolution} -g 50 -vf yadif  -r 25  -b:v %{bitrate}M -f mp4 \"%{output}\""
ffmpeg_ccmmand_addition_tmpl = " -pass %{pass_number} \"%{output}\""

ftp_server = config['ftp_server']
src_dir = config['local_directory']
dst_dir = "converted-" + Time.now.strftime("%Y.%m.%d-%H%M%S")
log_file_path = "#{dst_dir}/convertion.log"
extension = config['extension']
upload_dir = config['upload_directory']
bitrate_ratio = config['bitrate_ratio']


files = get_files_list(src_dir, extension)

if files.empty?
  puts "[-] There is no files with the appropriate extension (#{extension}) in #{dir_name}"
  exit
end

recreate_dir_structure(src_dir, dst_dir, extension)
log = File.open(log_file_path, "w")

puts "\n[i] Starting file processing.\n\n"

counter = 1
files.each do |file_name|

  puts "[+] Trying to get resolution of '#{file_name}'"

  resolution, maxrate, bitrate, audio_presence = nil
  exclude_from_convertion = false
  cut_start = 0
  cut_end = 0
  need_to_cut = false

  if special_files.keys.include? file_name 
    resolution = special_files[file_name]['resolution'] || get_video_resolution(file_name)
    bitrate = special_files[file_name]['bitrate']
    unless bitrate.nil?
      bitrate = bitrate.to_f 
      maxrate = (bitrate * 1.2).round(1)
    end
    if special_files[file_name].has_key? 'audio'
      audio_presence = special_files[file_name]['audio']
    end
    if special_files[file_name].has_key? 'exclude'
      exclude_from_convertion = special_files[file_name]['exclude']
    end
    if special_files[file_name].has_key? 'cut_start' and special_files[file_name].has_key? 'cut_end'
      need_to_cut = true
      cut_start = special_files[file_name]['cut_start']
      cut_end = special_files[file_name]['cut_end']
      puts cut_start
      puts cut_end
    end
  end

  resolution = resolution || get_video_resolution(file_name)

  puts "[+] Resolution: #{resolution}"

  maxrate, bitrate = get_rates(resolution, bitrate_ratio) if bitrate.nil?
  
  puts "[+] Bitrate: #{bitrate}k, Maxrate: #{maxrate}k"

  audio_presence = true if audio_presence.nil?
  puts "[+] Audio presence: #{audio_presence}"
  output_file = dst_dir + '/' + file_name


  ffmpeg_params = { filename: file_name, maxrate: maxrate, bitrate: bitrate, resolution: resolution }

  puts "[+] Starting video convertion."

  if exclude_from_convertion 
    puts "[~] Excluding '#{file_name}' from convertion. It will be copied to ftp server as is.\n\n"
    FileUtils.copy(file_name, output_file)
  else
    if not audio_presence
      puts "[~] Video convertion without audio progress"
      execute_ffmpeg("ffmpeg -y -i \"#{file_name}\" -an -vcodec libx264  -vprofile main -level 31 -maxrate #{maxrate}M -minrate 100k -bufsize 10000000 -s #{resolution} -g 50 -vf yadif  -r 25  -b:v #{bitrate}M -f mp4 \"#{output_file}\"")
    else
      puts "[~] Pass1 convertion progress"
      execute_ffmpeg((ffmpeg_command_main_tmpl % ffmpeg_params) + (ffmpeg_ccmmand_addition_tmpl % { pass_number: 1, output: '/dev/null'}))
      puts "[~] Pass2 convertion progress"
      execute_ffmpeg((ffmpeg_command_main_tmpl % ffmpeg_params) + (ffmpeg_ccmmand_addition_tmpl % { pass_number: 2, output: output_file }))
    end
    if need_to_cut
      cut_video(file_name, output_file, cut_start, cut_end)
    end
  end


  if not exclude_from_convertion
    puts "[~] MP4Box processing"
    mp4box_processing(output_file)
    puts "\n"  
  end

  log.puts ("#{counter}) " + get_meta_data(file_name).join(', ') + "\n")
  counter += 1
  

end

puts "[+] Converted! Local storage: #{dst_dir}\n\n"  

log.close

files = get_files_list(dst_dir, extension)

if files.empty?
   puts "[-] Nothing to upload"
   exit
end

puts "[i] Starting file upload.\n\n"

counter = 1
dst_dir_name_length = dst_dir.size + 1
files.each do |file_path|
  puts "#{counter}) Uploading '#{file_path}'"
  ftp_upload(ftp_server, file_path, upload_dir + '/' + file_path[dst_dir_name_length..file_path.size])
  counter += 1
end

puts "#{counter}) Uploading '#{log_file_path}'"
ftp_upload(ftp_server, log_file_path, upload_dir)

puts "\n[+] All files were uploaded to #{ftp_server}/#{upload_dir}\n\n"

