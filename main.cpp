#include <iostream>
#include "interpreter.h"
#include "buffer_manager.h"
#include "const.h"

// 全局缓冲区管理器，所有模块共享
BufferManager buffer_manager;

// 当前使用的数据库名称，空字符串表示使用默认路径
// 通过 USE <dbname> 命令切换
std::string current_database = "";

int main(int argc, const char* argv[]) {
    std::cout << ">>> Welcome to MiniSQL" << std::endl;
    // 主循环：不断读取SQL语句并执行
    while (1) {
        Interpreter query;
        query.getQuery();
        query.EXEC();
    }
    return 0;
}
