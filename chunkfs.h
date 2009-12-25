#ifndef __INCLUDE_CHUNKFS_H__
#define __INCLUDE_CHUNKFS_H__

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdarg.h>
#include <pthread.h>

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <fstream>


class ChunkFile;
typedef std::map<std::string, ChunkFile*> ChunkFileMap;
typedef enum {
  LOG_VERBOSE   = 0, LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR, LOG_FATAL,
  LOG_ALL = 0, LOG_DEVELOP = LOG_DEBUG,
  LOG_LEVELS    = 0x7FFF,
  LOG_NOHEADER  = 0x8000,
} LOGLEVEL;


class Log
{
public:
  Log(const char* header = NULL, FILE* fp = stdout, LOGLEVEL level = LOG_DEVELOP);
  inline virtual ~Log() {};
  inline void set_header(const char* header) { m_header = header; };
  inline void set_fp(FILE* fp) { m_log = fp; };
  inline void set_level(LOGLEVEL level) { m_level = level; };
  void vlog(int level, const char* fmt, va_list va) const;
  void operator() (int level, const char* fmt, ...) const;

private:
  LOGLEVEL  m_level;
  FILE*     m_log;
  const char* m_header;
};


class ChunkInfo
{
public:
  ChunkInfo(const char* base_dir);
  virtual ~ChunkInfo();
  inline const char* base_dir() const { return m_base_dir; };
  ChunkFile* find(const char* key);
  bool add(const char* key, ChunkFile* chunk);
  void erase(const char* key);

private:
  ChunkFileMap::iterator find_(const char* key);
  const char*   m_base_dir;
  ChunkFileMap  m_chunks;
  pthread_mutex_t m_chunk_lock;
};
  

class Chunk
{
public:
  Chunk(off_t cnk_start, off_t cnk_end, const char* path);
  inline virtual ~Chunk() {};
  bool operator< (const Chunk& obj) const;
  bool inbound(off_t offset) const;
  inline off_t chk_start() const { return m_start; };
  inline off_t cnk_end() const { return m_end; };
  ssize_t read(char* buf, size_t count, off_t offset) const;
  void info(const char*& path, off_t& cnk_start, off_t& cnk_end) const;

private:
  off_t m_start;
  off_t m_end;
  std::string m_path;
};


class ChunkFile
{
public:
  ChunkFile(int& errcode, const char* path, const char* function = "", bool buildmap = false);
  inline virtual ~ChunkFile() {};

  Log log;
  inline void set_function(const char* function = "") { log.set_header(function); };
  inline const struct stat* stat() const { return &m_stat; };
  ssize_t read(int& errcode, char* buf, size_t count, off_t offset);
  inline void unlocked_increment_reference() { m_references--; };
  inline long unlocked_decrement_reference() {
    if(m_references<=0) {
      log(LOG_FATAL, "*** m_references<=0 ***\n");
      dump();
    }
    return --m_references;
  };
  void dump(int level = LOG_FATAL|LOG_NOHEADER) const;

private:
  struct stat m_stat;
  std::string m_base, m_map;
  std::vector<Chunk> m_chunks;
  long m_references;

  int setup(const char* path, bool buildmap);
  int root_setup();
  int subdir_setup(bool buildmap);
  int build_chunk_vector(std::ifstream& map_file);
  bool find_chunk(off_t offset, std::vector<Chunk>::iterator& it);
  int file_exist(const char* path, struct stat* p_stat = NULL) const;
  const char* get_token(char* line, char*& next) const;
  off_t get_value(char* line, char*& next, off_t default_value = 0) const;
};


class ChunkFsConfig
{
public:
  inline ChunkFsConfig() {};
  inline virtual ~ChunkFsConfig() {};
  void parse_args(int& argc, char* argv[]);
  inline const char* root() const { return m_root.c_str(); };
  std::string base_dir(const char* path) const;
  std::string map_file(const char* path) const;

private:
  std::string m_root;
  std::string m_map_suffix;
  void parsearg_helper(std::string& opt, int& argc, char** argv, int& it) const {
    opt = argv[it+1];
    memmove(argv+it, argv+it+2, sizeof(argv)*(argc-it));
    argc -= 2;
    it--;
  };
  std::string id_to_path(const char* id) const;
};

#endif // __INCLUDE_CHUNKFS_H__
