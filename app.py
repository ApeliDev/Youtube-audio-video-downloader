from flask import Flask, request, jsonify, send_file, render_template, make_response, send_from_directory, Response
from pytube import YouTube, exceptions
from pytube.cli import on_progress
import os
import uuid
import re
import logging
from flask_cors import CORS
from werkzeug.utils import secure_filename
import time
from datetime import timedelta
import threading
import shutil
from functools import wraps
import sqlite3
import json
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, APIC
from mutagen.mp3 import MP3
import requests
from mutagen.mp4 import MP4, MP4Cover
import ffmpeg
import yt_dlp

import subprocess
import tempfile
import zipfile
import stat
import platform
import re
import shutil
import sqlite3

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)  

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max upload size
app.config['DOWNLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
app.config['AUDIO_FOLDER'] = os.path.join(app.config['DOWNLOAD_FOLDER'], 'audio')
app.config['VIDEO_FOLDER'] = os.path.join(app.config['DOWNLOAD_FOLDER'], 'video')
app.config['TEMP_FOLDER'] = os.path.join(app.config['DOWNLOAD_FOLDER'], 'temp')
app.config['MAX_FILE_AGE'] = timedelta(hours=24)  # Files older than this will be deleted
app.config['THUMBNAIL_FOLDER'] = os.path.join(app.config['DOWNLOAD_FOLDER'], 'thumbnails')
app.config['DATABASE'] = os.path.join(app.config['DOWNLOAD_FOLDER'], 'media.db')

# Quality presets
QUALITY_PRESETS = {
    'audio': {
        'ultra': {'abr': '256kbps', 'ext': 'mp3', 'mime': 'audio/mpeg'},
        'high': {'abr': '160kbps', 'ext': 'mp3', 'mime': 'audio/mpeg'},
        'medium': {'abr': '128kbps', 'ext': 'mp3', 'mime': 'audio/mpeg'},
        'low': {'abr': '64kbps', 'ext': 'mp3', 'mime': 'audio/mpeg'},
        'opus': {'abr': '160kbps', 'ext': 'opus', 'mime': 'audio/ogg'}
    },
    'video': {
        '4k': {'res': '2160p', 'ext': 'mp4', 'mime': 'video/mp4'},
        '1080p': {'res': '1080p', 'ext': 'mp4', 'mime': 'video/mp4'},
        '720p': {'res': '720p', 'ext': 'mp4', 'mime': 'video/mp4'},
        '480p': {'res': '480p', 'ext': 'mp4', 'mime': 'video/mp4'},
        '360p': {'res': '360p', 'ext': 'mp4', 'mime': 'video/mp4'},
        'webm': {'res': '720p', 'ext': 'webm', 'mime': 'video/webm'}
    }
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


for folder in [
    app.config['DOWNLOAD_FOLDER'],
    app.config['AUDIO_FOLDER'],
    app.config['VIDEO_FOLDER'],
    app.config['TEMP_FOLDER'],
    app.config['THUMBNAIL_FOLDER']
]:
    os.makedirs(folder, exist_ok=True)

# Initialize database
def init_db():
    with sqlite3.connect(app.config['DATABASE']) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS media (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                duration INTEGER,
                size INTEGER,
                format TEXT,
                type TEXT,
                quality TEXT,
                thumbnail TEXT,
                path TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                youtube_id TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS playlist_items (
                playlist_id TEXT NOT NULL,
                media_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (playlist_id, media_id),
                FOREIGN KEY (playlist_id) REFERENCES playlists(id),
                FOREIGN KEY (media_id) REFERENCES media(id)
            )
        ''')
        conn.commit()

init_db()

# Database functions
def get_db():
    return sqlite3.connect(app.config['DATABASE'])

def add_media_to_db(media_data):
    try:
        with get_db() as conn:
            conn.execute('''
                INSERT INTO media (id, title, author, duration, size, format, type, quality, thumbnail, path, youtube_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                media_data['id'],
                media_data['title'],
                media_data.get('author'),
                media_data.get('duration'),
                media_data.get('size'),
                media_data.get('format'),
                media_data.get('type'),
                media_data.get('quality'),
                media_data.get('thumbnail'),
                media_data['path'],
                media_data.get('youtube_id')
            ))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding media to database: {str(e)}")
        return False

def get_media_from_db(media_id):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM media WHERE id = ?', (media_id,))
            row = cursor.fetchone()
            if row:
                columns = [column[0] for column in cursor.description]
                return dict(zip(columns, row))
        return None
    except Exception as e:
        logger.error(f"Error getting media from database: {str(e)}")
        return None

def get_all_media_from_db():
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM media ORDER BY created_at DESC')
            rows = cursor.fetchall()
            if rows:
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
        return []
    except Exception as e:
        logger.error(f"Error getting all media from database: {str(e)}")
        return []

def delete_media_from_db(media_id):
    try:
        with get_db() as conn:
            # get the media info to delete the file
            media = get_media_from_db(media_id)
            if media:
                # Delete the file
                if os.path.exists(media['path']):
                    os.remove(media['path'])
                
                # Delete from database
                conn.execute('DELETE FROM media WHERE id = ?', (media_id,))
                conn.execute('DELETE FROM playlist_items WHERE media_id = ?', (media_id,))
                conn.commit()
                return True
        return False
    except Exception as e:
        logger.error(f"Error deleting media from database: {str(e)}")
        return False

def get_storage_info():
    try:
        total, used, free = shutil.disk_usage(app.config['DOWNLOAD_FOLDER'])
        return {
            'total': total,
            'used': used,
            'free': free
        }
    except Exception as e:
        logger.error(f"Error getting storage info: {str(e)}")
        return {
            'total': 0,
            'used': 0,
            'free': 0
        }

# Rate limiting decorator
def rate_limit(limit=5, per=60):
    def decorator(f):
        requests = []

        @wraps(f)
        def wrapped(*args, **kwargs):
            now = time.time()
            requests.append(now)
            
            # Remove old requests
            requests[:] = [req for req in requests if now - req < per]
            
            if len(requests) > limit:
                return jsonify({
                    'error': 'Too many requests',
                    'message': f'Rate limit exceeded: {limit} requests per {per} seconds'
                }), 429
            return f(*args, **kwargs)
        return wrapped
    return decorator

# Cleanup old files in the background
def cleanup_old_files():
    while True:
        try:
            now = time.time()
            for folder in [app.config['AUDIO_FOLDER'], app.config['VIDEO_FOLDER'], app.config['TEMP_FOLDER']]:
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if os.path.isfile(file_path):
                        file_age = now - os.path.getmtime(file_path)
                        if file_age > app.config['MAX_FILE_AGE'].total_seconds():
                            try:
                                os.remove(file_path)
                                logger.info(f"Deleted old file: {file_path}")
                            except Exception as e:
                                logger.error(f"Error deleting file {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error in cleanup thread: {str(e)}")
        time.sleep(3600)  

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_files, daemon=True)
cleanup_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/video-info', methods=['GET'])
def get_video_info():
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'URL parameter is required'}), 400
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            formats = []
            for f in info['formats']:
                if f.get('vcodec') != 'none':  # Video formats
                    formats.append({
                        'itag': f['format_id'],
                        'resolution': f.get('resolution', 'unknown'),
                        'fps': f.get('fps'),
                        'ext': f['ext'],
                        'filesize': f.get('filesize')
                    })
                elif f.get('acodec') != 'none':  # Audio formats
                    formats.append({
                        'itag': f['format_id'],
                        'abr': f.get('abr', 0),
                        'ext': f['ext'],
                        'filesize': f.get('filesize')
                    })
            
            return jsonify({
                'title': info['title'],
                'author': info.get('uploader'),
                'length': info['duration'],
                'thumbnail_url': info['thumbnail'],
                'views': info.get('view_count'),
                'video_id': info['id'],
                'formats': formats
            })
            
    except Exception as e:
        logger.error(f"Error fetching video info: {str(e)}")
        return jsonify({'error': str(e)}), 500


def ensure_ffmpeg():
    """Check if FFmpeg is installed, if not attempt to download and install it"""
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, 
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except:
        # Download FFmpeg for Windows
        if platform.system() == 'Windows':
            ffmpeg_url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
            temp_dir = tempfile.gettempdir()
            
            try:
                logger.info("Attempting to download FFmpeg...")
                
                # Download
                response = requests.get(ffmpeg_url, stream=True)
                zip_path = os.path.join(temp_dir, 'ffmpeg.zip')
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                
                # Extract
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find ffmpeg.exe
                for root, dirs, files in os.walk(temp_dir):
                    if 'ffmpeg.exe' in files:
                        ffmpeg_path = os.path.join(root, 'ffmpeg.exe')
                        
                        # Make executable
                        st = os.stat(ffmpeg_path)
                        os.chmod(ffmpeg_path, st.st_mode | stat.S_IEXEC)
                        
                        # Add to PATH
                        ffmpeg_dir = os.path.dirname(ffmpeg_path)
                        os.environ['PATH'] += os.pathsep + ffmpeg_dir
                        
                        # Verify installation
                        try:
                            subprocess.run(['ffmpeg', '-version'], check=True,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            logger.info("FFmpeg installed successfully")
                            return True
                        except:
                            logger.error("FFmpeg installation verification failed")
                            return False
                
                logger.error("Could not find ffmpeg.exe in downloaded files")
                return False
                
            except Exception as e:
                logger.error(f"Failed to auto-install FFmpeg: {str(e)}")
                return False
        else:
            logger.error("Automatic FFmpeg installation only supported on Windows")
            return False

@app.route('/api/download', methods=['POST'])
@rate_limit(limit=3, per=60)
def download_from_youtube():
    # Check for FFmpeg
    if not ensure_ffmpeg():
        return jsonify({
            'error': 'FFmpeg required',
            'message': 'Could not automatically install FFmpeg. Please install manually from https://ffmpeg.org/'
        }), 500

    data = request.json
    url = data.get('url')
    download_type = data.get('download_type', 'audio').lower()
    quality = data.get('quality', 'best')
    filename = data.get('filename')
    include_metadata = data.get('metadata', True)
    trim_start = data.get('trim_start')
    trim_end = data.get('trim_end')
    
    if not url:
        return jsonify({'error': 'URL is required'}), 400
    
    try:
        # Clean URL
        url = url.replace('%3D', '=').replace('%26', '&')
        
        # Get video info first to determine title
        ydl_info = yt_dlp.YoutubeDL({'quiet': True, 'extract_flat': True})
        info = ydl_info.extract_info(url, download=False)
        
        # Generate filename
        safe_title = secure_filename(re.sub(r'[^\w\-_\. ]', '', info.get('title', 'video')))
        unique_id = str(uuid.uuid4())[:8]
        if not filename:
            filename = f"{safe_title}_{unique_id}"
        else:
            filename = secure_filename(filename)
        
        # Set download options
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'outtmpl': os.path.join(
                app.config['AUDIO_FOLDER' if download_type == 'audio' else 'VIDEO_FOLDER'],
                filename + '.%(ext)s'
            ),
            'postprocessors': [],
            'merge_output_format': 'mp4',
            'writethumbnail': True,
            'ffmpeg_location': os.path.dirname(subprocess.check_output(['which', 'ffmpeg']).decode().strip())
        }
        
        # Configure format selection
        if download_type == 'audio':
            ydl_opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [
                    {
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    },
                    {
                        'key': 'FFmpegMetadata',
                        'add_metadata': True
                    }
                ]
            })
        else:
            if quality == 'highest':
                ydl_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
            else:
                res = quality.replace('p', '')
                ydl_opts['format'] = f'bestvideo[height<={res}][ext=mp4]+bestaudio[ext=m4a]/best[height<={res}][ext=mp4]/best'
        
        # Handle video trimming
        if trim_start or trim_end:
            def parse_time(time_str):
                parts = list(map(int, time_str.split(':')))
                if len(parts) == 3:  # HH:MM:SS
                    return parts[0] * 3600 + parts[1] * 60 + parts[2]
                elif len(parts) == 2:  # MM:SS
                    return parts[0] * 60 + parts[1]
                return int(time_str)  # SS
            
            if trim_start and trim_end:
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': 'mp4',
                    'when': 'before_dl',
                    'pre_opts': [
                        '-ss', str(parse_time(trim_start)),
                        '-to', str(parse_time(trim_end))
                    ]
                })
            elif trim_start:
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': 'mp4',
                    'when': 'before_dl',
                    'pre_opts': ['-ss', str(parse_time(trim_start))]
                })
            elif trim_end:
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': 'mp4',
                    'when': 'before_dl',
                    'pre_opts': ['-to', str(parse_time(trim_end))]
                })
        
        # Download the file
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            downloaded_file = ydl.prepare_filename(info)
            
            if download_type == 'audio':
                downloaded_file = downloaded_file.replace('.webm', '.mp3').replace('.m4a', '.mp3')
            
            # Handle metadata and thumbnail
            if include_metadata:
                try:
                    if download_type == 'audio':
                        audio = MP3(downloaded_file, ID3=EasyID3)
                        audio['title'] = info['title']
                        audio['artist'] = info.get('uploader', 'Unknown')
                        audio['album'] = 'YouTube Download'
                        audio.save()
                        
                        # Add thumbnail
                        audio = MP3(downloaded_file, ID3=ID3)
                        thumb_path = downloaded_file.replace('.mp3', '.webp')
                        if os.path.exists(thumb_path):
                            with open(thumb_path, 'rb') as thumb_file:
                                audio.tags.add(APIC(
                                    encoding=3,
                                    mime='image/webp',
                                    type=3,
                                    desc='Cover',
                                    data=thumb_file.read()
                                ))
                                audio.save()
                            os.remove(thumb_path)
                    else:
                        video = MP4(downloaded_file)
                        video['\xa9nam'] = info['title']
                        video['\xa9ART'] = info.get('uploader', 'Unknown')
                        thumb_path = downloaded_file.replace('.mp4', '.webp')
                        if os.path.exists(thumb_path):
                            with open(thumb_path, 'rb') as thumb_file:
                                video['covr'] = [MP4Cover(thumb_file.read(), imageformat=MP4Cover.FORMAT_JPEG)]
                            os.remove(thumb_path)
                        video.save()
                except Exception as e:
                    logger.warning(f"Metadata error: {str(e)}")
        
        # Add to database
        media_id = str(uuid.uuid4())
        media_data = {
            'id': media_id,
            'title': info['title'],
            'author': info.get('uploader'),
            'duration': info.get('duration'),
            'size': os.path.getsize(downloaded_file),
            'format': 'mp3' if download_type == 'audio' else 'mp4',
            'type': download_type,
            'quality': quality,
            'thumbnail': info.get('thumbnail'),
            'path': downloaded_file,
            'youtube_id': info.get('id')
        }
        
        if not add_media_to_db(media_data):
            logger.error("Failed to add media to database")
        
        return jsonify({
            'success': True,
            'message': 'Download completed successfully',
            'title': info['title'],
            'download_url': f"/media/{media_id}",
            'file_type': 'mp3' if download_type == 'audio' else 'mp4',
            'mime_type': 'audio/mpeg' if download_type == 'audio' else 'video/mp4',
            'file_size': os.path.getsize(downloaded_file),
            'duration': info.get('duration'),
            'media_id': media_id,
            'thumbnail_url': info.get('thumbnail')
        })
        
    except yt_dlp.utils.DownloadError as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({'error': 'Failed to download video. YouTube may have blocked the request.'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({'error': str(e)}), 500

    

@app.route('/media/<media_id>')
def serve_media(media_id):
    media = get_media_from_db(media_id)
    if not media or not os.path.exists(media['path']):
        return jsonify({'error': 'Media not found'}), 404
    
    # Determine MIME type from file extension
    ext = os.path.splitext(media['path'])[1].lower().lstrip('.')
    mime_types = {
        'mp3': 'audio/mpeg',
        'mp4': 'video/mp4',
        'webm': 'video/webm',
        'opus': 'audio/ogg'
    }
    mimetype = mime_types.get(ext, 'application/octet-stream')
    
    # Stream the file for playback
    range_header = request.headers.get('Range', None)
    if not range_header:
        return send_file(media['path'], mimetype=mimetype)
    
    # Handle range requests for streaming
    size = os.path.getsize(media['path'])
    byte1, byte2 = 0, None
    
    range_ = range_header.split('=')[1]
    if '-' in range_:
        byte1, byte2 = range_.split('-')
        byte1 = int(byte1)
        if byte2:
            byte2 = int(byte2)
        else:
            byte2 = size - 1
    
    length = byte2 - byte1 + 1 if byte2 else size - byte1
    
    with open(media['path'], 'rb') as f:
        f.seek(byte1)
        data = f.read(length)
    
    response = Response(
        data,
        206,
        mimetype=mimetype,
        direct_passthrough=True
    )
    
    response.headers.add('Content-Range', f'bytes {byte1}-{byte2 if byte2 else size-1}/{size}')
    response.headers.add('Accept-Ranges', 'bytes')
    response.headers.add('Content-Length', str(length))
    
    return response

@app.route('/download/<media_id>')
def download_media(media_id):
    media = get_media_from_db(media_id)
    if not media or not os.path.exists(media['path']):
        return jsonify({'error': 'Media not found'}), 404
    
    # Determine MIME type from file extension
    ext = os.path.splitext(media['path'])[1].lower().lstrip('.')
    mime_types = {
        'mp3': 'audio/mpeg',
        'mp4': 'video/mp4',
        'webm': 'video/webm',
        'opus': 'audio/ogg'
    }
    mimetype = mime_types.get(ext, 'application/octet-stream')
    
    # Stream the file for download
    response = make_response(send_file(
        media['path'],
        mimetype=mimetype,
        as_attachment=True,
        download_name=f"{media['title']}.{ext}"
    ))
    
    # cache headers to prevent caching of downloads
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    
    return response

@app.route('/api/media-library')
def get_media_library():
    try:
        media_files = get_all_media_from_db()
        storage_info = get_storage_info()
        
        return jsonify({
            'success': True,
            'files': media_files,
            'storage': storage_info
        })
    except Exception as e:
        logger.error(f"Error getting media library: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/media/<media_id>', methods=['DELETE'])
def delete_media(media_id):
    try:
        if delete_media_from_db(media_id):
            return jsonify({'success': True, 'message': 'Media deleted successfully'})
        return jsonify({'error': 'Media not found'}), 404
    except Exception as e:
        logger.error(f"Error deleting media: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cleanup', methods=['POST'])
def cleanup_files():
    try:
        # Delete all files in audio and video folders
        for folder in [app.config['AUDIO_FOLDER'], app.config['VIDEO_FOLDER']]:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}: {str(e)}")
        
        # Clear database
        with get_db() as conn:
            conn.execute('DELETE FROM media')
            conn.execute('DELETE FROM playlist_items')
            conn.commit()
        
        return jsonify({'success': True, 'message': 'All download files have been cleaned up'})
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)