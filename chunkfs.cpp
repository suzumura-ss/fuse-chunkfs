#include "chunkfs.h"

// Globals.
static ChunkInfo* get_chunk_info()
{
  struct fuse_context* fc = fuse_get_context();
  return (ChunkInfo*)(fc->private_data);
}
static Log glog;
static ChunkFsConfig gChunkFsConfig;


// Log class imprements.
Log::Log(const char* header, FILE* fp, LOGLEVEL level)
{
  m_header = header;
  m_log = fp;
  m_level = level;
}

void Log::vlog(int level, const char* fmt, va_list va) const
{
  if((level&LOG_LEVELS)<m_level) return;
  if((level&LOG_NOHEADER)==0 && m_header) fprintf(m_log, "[%s] ", m_header);
  vfprintf(m_log, fmt, va);
  fflush(m_log);
}

void Log::operator() (int level, const char* fmt, ...) const
{
  va_list va;
  va_start(va, fmt);
  vlog(level, fmt, va);
  va_end(va);
}


// ChunkInfo class imprements.
ChunkInfo::ChunkInfo(const char* base_dir)
{
  m_base_dir = base_dir;
  pthread_mutex_init(&m_chunk_lock, NULL);
  m_chunks.clear();
}

ChunkInfo::~ChunkInfo()
{
  while(!m_chunks.empty()) {
    ChunkFileMap::iterator it = m_chunks.begin();
    if(it->second) {
      glog(LOG_FATAL, "[%s] deleteing %p for %s\n", __FUNCTION__, &(*it), it->first.c_str());
      it->second->dump();
      delete it->second;
      m_chunks.erase(it);
    }
  }
  m_chunks.clear();
}

ChunkFile* ChunkInfo::find(const char* key)
{
  ChunkFile* result = NULL;
  pthread_mutex_lock(&m_chunk_lock);
  {
    ChunkFileMap::iterator it = find_(key);
    if(it!=m_chunks.end()) result = it->second;
  }
  pthread_mutex_unlock(&m_chunk_lock);
  return result;
}

bool ChunkInfo::add(const char* key, ChunkFile* chunk)
{
  std::string key_(key);
  std::pair<std::string, ChunkFile*> p(key_, chunk);
  std::pair<ChunkFileMap::iterator, bool> result;
  pthread_mutex_lock(&m_chunk_lock);
  {
    ChunkFileMap::iterator it = find_(key);
    if(it==m_chunks.end()) {
      result = m_chunks.insert(p);
    } else {
      delete chunk;
      it->second->unlocked_increment_reference();
      result.second = true;
    }
  }
  pthread_mutex_unlock(&m_chunk_lock);
  return result.second;
}

void ChunkInfo::erase(const char* key)
{
  pthread_mutex_lock(&m_chunk_lock);
  {
    ChunkFileMap::iterator it = find_(key);
    if(it!=m_chunks.end()) {
      if(it->second->unlocked_decrement_reference()==0) {
        delete it->second;
        m_chunks.erase(it);
      }
    }
  }
  pthread_mutex_unlock(&m_chunk_lock);
}

ChunkFileMap::iterator ChunkInfo::find_(const char* key)
{
  std::string key_(key);
  return m_chunks.find(key_);
}

  
// Chunk class imprements.
Chunk::Chunk(off_t cnk_start, off_t cnk_end, const char* path)
{
  m_start = cnk_start;
  m_end   = cnk_end;
  m_path  = path;
}

bool Chunk::operator< (const Chunk& obj) const
{
  return m_start < obj.m_start;
}

bool Chunk::inbound(off_t offset) const
{
  return (m_start<=offset && offset<=m_end)? true: false;
}

ssize_t Chunk::read(char* buf, size_t count, off_t offset) const
{
  if(offset<m_start || offset>m_end) return 0;
  int fd = ::open(m_path.c_str(), O_RDONLY);
  ssize_t rlen = -1;
  if(fd>=0) {
    ::lseek(fd, SEEK_SET, offset-m_start);
    rlen = ::read(fd, buf, count);
    ::close(fd);
  }
  return rlen;
}

void Chunk::info(const char*& path, off_t& cnk_start, off_t& cnk_end) const
{
  path = m_path.c_str();
  cnk_start = m_start;
  cnk_end   = m_end;
}


// ChunkFile class imprements.
ChunkFile::ChunkFile(int& errcode, const char* path, const char* function, bool buildmap)
{
  set_function(function);
  m_references = 1;
  errcode = setup(path, buildmap);
}

ssize_t ChunkFile::read(int& errcode, char* buf, size_t count, off_t offset)
{
  ssize_t read = 0;
  while(count>0) {
    log(LOG_VERBOSE, "Reading to %p: offset=%ld, count=%d, read=%d.\n", buf, offset, count, read);
    std::vector<Chunk>::iterator it;
    if(find_chunk(offset, it)) {
      ssize_t r = it->read(buf, count, offset);
      if(r==0) {
        // ???
        errcode = ENOENT;
        log(LOG_FATAL, "Unknown state. invalid offset:%ld.\n", offset);
      }
      if(r<0) {
        // Read failed.
        errcode = errno;
        log(LOG_ERROR, "Read faild. offset=%ld, err=%d.\n", offset, errcode);
        return -1;
      }
      buf += r;
      read += r;
      count -= r;
      offset += r;
    } else {
      log(LOG_INFO, "Chunk not found. offset=%ld.\n", offset);
      if(read>0) return read;
      errcode = EINVAL;
      return -1;
    }
  }
  return read;
}

int ChunkFile::setup(const char* path, bool buildmap)
{
  memset(&m_stat, 0, sizeof(m_stat));
  m_base = gChunkFsConfig.base_dir(path);
  m_map  = gChunkFsConfig.map_file(path);
  return ((strcmp(path, "/")!=0) ? subdir_setup(buildmap) : root_setup());
}

int ChunkFile::root_setup()
{
  log(LOG_DEBUG, "root_setup() => %s\n", m_map.c_str());
  m_stat.st_mode = S_IFDIR | 0555;
  m_stat.st_nlink = 2;
  return 0;
}

int ChunkFile::subdir_setup(bool buildmap)
{
  log(LOG_DEBUG, "subdir_setup(%d) => %s\n", buildmap, m_map.c_str());
  int errcode = 0;
  struct stat buf;
  errcode = file_exist(m_map.c_str(), &buf);
  if(errcode!=0) return errcode;
  memcpy(&m_stat, &buf, sizeof(m_stat));
  std::ifstream map_file(m_map.c_str());
  char  line[1024], *next = NULL;

  // file size, chunk-vector
  map_file.getline(line, sizeof(line), '\n');
  if(buildmap) {
    errcode = build_chunk_vector(map_file);
    m_stat.st_size = (errcode==0) ? get_value(line, next) : 0;
  } else {
    m_stat.st_size = get_value(line, next);
  }

  // ACL, hard-links
  if(errcode==0) {
    m_stat.st_mode = S_IFREG | 0444;
    m_stat.st_nlink = 1;
  }

  return errcode;
}

int ChunkFile::build_chunk_vector(std::ifstream& map_file)
{
  int errcode = 0;
  while(!map_file.eof()) {
    char  line[1024], *next = NULL;
    map_file.getline(line, sizeof(line), '\n');
    off_t cnk_start = get_value(line, next, (off_t)-1);
    off_t cnk_end   = get_value(NULL, next, (off_t)-1);
    std::string cnk_file = get_token(NULL, next)?: "";
    if(cnk_start!=(off_t)-1 && cnk_end!=(off_t)-1 && (!cnk_file.empty())) {
      snprintf(line, sizeof(line), "%s/%s", m_base.c_str(), cnk_file.c_str());
      errcode = file_exist(line);
      if(errcode==0) {
        log(LOG_VERBOSE, "-- %s(): Add chunk: %ld-%ld:%s\n", __FUNCTION__, cnk_start, cnk_end, line);
        m_chunks.push_back(Chunk(cnk_start, cnk_end, line));
      } else {
        log(LOG_ERROR, "-- %s(): Chunk file not exist: %ld-%ld:%s\n", __FUNCTION__, cnk_start, cnk_end, line);
        m_chunks.clear();
        return errcode;
      }
    }
  }
  std::sort(m_chunks.begin(), m_chunks.end());

  std::vector<Chunk>::iterator it;
  int ai;
  for(ai = 0, it = m_chunks.begin() ;it != m_chunks.end() ;ai++, it++) {
    off_t cnk_start, cnk_end;
    const char* path;
    it->info(path, cnk_start, cnk_end);
    log(LOG_VERBOSE, "%4d: %10ld - %10ld %s\n", ai, cnk_start, cnk_end, path);
  }

  return errcode;
}

bool ChunkFile::find_chunk(off_t offset, std::vector<Chunk>::iterator& it)
{
  Chunk obj(offset, offset, "");
  if(m_chunks.empty()) {
    log(LOG_FATAL, "-- %s(): Chunk is empty: ofset=%ld\n", __FUNCTION__, offset);
    return false;
  }
  it = std::lower_bound(m_chunks.begin(), m_chunks.end(), obj);
  if(it==m_chunks.end()) it--;
  if(it->inbound(offset)) {
    const char* path;
    off_t cnk_start, cnk_end;
    it->info(path, cnk_start, cnk_end);
    log(LOG_VERBOSE, "-- %s(): Chunk found: offset=%ld (%ld-%ld:%s)\n", 
                __FUNCTION__, offset, cnk_start, cnk_end, path);
    return true;
  }
  log(LOG_VERBOSE, "-- %s(): Chunk not found: ofset=%ld\n", __FUNCTION__, offset);
  return false;
}

int ChunkFile::file_exist(const char* path, struct stat* p_stat) const
{
  struct stat buf;
  ::stat(path, p_stat?: &buf);
  return errno;
}

const char* ChunkFile::get_token(char* line, char*& next) const
{
  if(line==NULL && next==NULL) return NULL;
  log(LOG_VERBOSE, "-- %s(\"%s\", %p)", __FUNCTION__, line, next);
  char* p = strtok_r(next? NULL: line, " ", &next);
  log(LOG_VERBOSE|LOG_NOHEADER, " => \"%s\"\n", p);
  if(!p) next = NULL;
  return p;
}

off_t ChunkFile::get_value(char* line, char*& next, off_t default_value) const
{
  const char* p = get_token(line, next);
  if(p) return (off_t)atoll(p);
  return default_value;
}

void ChunkFile::dump(int level) const
{
  glog(level, "m_base: %s\n", m_base.c_str());
  glog(level, "m_map:  %s\n", m_map.c_str());
  glog(level, "m_references: %d\n", m_references);
  glog(level, "m_chunks: %d chunks\n", m_chunks.size());
}


// Static functions
extern "C" {
  static int chunk_getattr(const char* path, struct stat *stbuf);
  static int chunk_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *ffi);
  static int chunk_open(const char* path, struct fuse_file_info* ffi);
  static int chunk_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* ffi);
  static int chunk_release(const char* path, struct fuse_file_info* ffi);
  static void* chunk_init(struct fuse_conn_info* fci);
  static void chunk_destroy(void* user_data);
}


static int chunk_getattr(const char* path, struct stat *stbuf)
{
  glog(LOG_DEBUG, ">> %s(%s)\n", __FUNCTION__, path);
  int errcode = 0;
  ChunkFile cf(errcode, path, __FUNCTION__);

  memset(stbuf, 0, sizeof(struct stat));
  if(errcode==0) memcpy(stbuf, cf.stat(), sizeof(*stbuf));

  return -errcode;
}


static int chunk_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *ffi)
{
  glog(LOG_DEBUG, ">> %s(%s)%p\n", __FUNCTION__, path);

  if(strcmp(path, "/")==0) {
    filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
    return 0;
  }

  return -ENOENT;
}


static int chunk_open(const char* path, struct fuse_file_info* ffi)
{
  glog(LOG_DEBUG, ">> %s(%s) ffi=%p, fh=%lld\n", __FUNCTION__, path, ffi, ffi->fh);
  int errcode = 0;
  ChunkInfo* ci = get_chunk_info();
  ChunkFile* cf = new ChunkFile(errcode, path, __FUNCTION__, true);
  if(!cf) errcode = ENOMEM;
  if(errcode==0) ci->add(path, cf);
  return -errcode;
}


static int chunk_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* ffi)
{
  glog(LOG_DEBUG, ">> %s(%s) ffi=%p, fh=%lld, size=%ld, offset=%ld\n", __FUNCTION__, path, ffi, ffi->fh, size, offset);
  ChunkInfo* ci = get_chunk_info();
  ChunkFile* cf =  ci->find(path);
  if(cf==NULL) {
    glog(LOG_FATAL, "Chunk %s is not found in map.\n", path);
    return -ENOENT;
  }
  cf->set_function(__FUNCTION__);
  int errcode = 0;
  int result = cf->read(errcode, buf, size, offset);

  return (result>=0)? result: -errcode;
}


static int chunk_release(const char* path, struct fuse_file_info* ffi)
{
  glog(LOG_DEBUG, ">> %s(%s) ffi=%p, fh=%lld\n", __FUNCTION__, path, ffi, ffi->fh);
  ChunkInfo* ci = get_chunk_info();
  ci->erase(path);

  return 0;
}


static void* chunk_init(struct fuse_conn_info* fci)
{
  fci->async_read   = 1;
  fci->max_write    = 0;
  fci->max_readahead = 0x20000;

  ChunkInfo* ci = new ChunkInfo(gChunkFsConfig.root());

  glog(LOG_DEBUG, "[%s] fci=%p, ci=%p\n", __FUNCTION__, fci, ci);
  return (void*)ci;
}


static void chunk_destroy(void* user_data)
{
  glog(LOG_DEBUG, "[%s] user_data=%p\n", __FUNCTION__, user_data);

  ChunkInfo* ci = (ChunkInfo*)user_data;
  delete ci;
}



int main(int argc, char* argv[])
{
  static struct fuse_operations chunk_oper;

  memset(&chunk_oper, 0, sizeof(chunk_oper));
  chunk_oper.getattr    = chunk_getattr;
  chunk_oper.readdir    = chunk_readdir;
  chunk_oper.open       = chunk_open;
  chunk_oper.read       = chunk_read;
  chunk_oper.release    = chunk_release;
  chunk_oper.init       = chunk_init;
  chunk_oper.destroy    = chunk_destroy;

  gChunkFsConfig.parse_args(argc, argv);

  // g_log = fopen("/var/log/chunkfs/access.log", "w+");
  return fuse_main(argc, argv, &chunk_oper, NULL);
}


// Configurator
void ChunkFsConfig::parse_args(int& argc, char* argv[])
{
  m_root = "/data/files/contents";
  m_map_suffix = "__CHUNK__";

  for(int it=1; it<argc; it++) {
    if(strcmp(argv[it], "-r")==0 && it+1<argc) {
      parsearg_helper(m_root, argc, argv, it);
    } else
    if(strcmp(argv[it], "--chunk")==0 && it+1<argc) {
      parsearg_helper(m_map_suffix, argc, argv, it);
    }
  }
  glog(LOG_VERBOSE, "root dir: %s\n", m_root.c_str());
}

std::string ChunkFsConfig::id_to_path(const char* id) const
{
  if(id==NULL) return std::string("/");

  char buf[20+20+5];
  off_t i = (off_t)atoll(id);
  off_t g = i / 1000;
  off_t k = g % 1000; g /= 1000;
  off_t m = g % 1000; g /= 1000;
  snprintf(buf, sizeof(buf), "%ld/%ld/%ld/%ld/", g, m, k, i);
  return std::string(buf);
}

std::string ChunkFsConfig::base_dir(const char* path) const
{
  std::string result = m_root.c_str();
  result += "/";

  char buf[strlen(path)];
  strcpy(buf, path+1);
  char* next;
  char* id = strtok_r(buf, "/", &next);   // contents id.
  // char* fn = strtok_r(NULL, "/", &next);  // filebody.
  result += id_to_path(id); 
  return result;
}

std::string ChunkFsConfig::map_file(const char* path) const
{
  return base_dir(path) + m_map_suffix;
}
