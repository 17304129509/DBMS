#ifndef _CONST_H_
#define _CONST_H_ 1

#include <string>

// 每一页的大小为4KB，是缓冲区管理和磁盘I/O的基本单位
#define PAGESIZE 4096
// 缓冲区中最多缓存的页数
#define MAXFRAMESIZE 100

// 当前使用的数据库名称
// 为空字符串时，表数据存放在默认路径 ./database/data/
//              catalog存放在默认路径 ./database/catalog/
//              index存放在默认路径 ./database/index/
// 设置为某个数据库名后，路径变为 ./database/<db_name>/data/ 等
// 通过 USE <dbname> 命令切换
extern std::string current_database;

#endif
