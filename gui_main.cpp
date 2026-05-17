// gui_main.cpp - MiniSQL GUI程序入口
// 初始化Qt应用程序和全局变量，创建主窗口并启动事件循环。
//
// 全局变量说明：
// - buffer_manager: 缓冲区管理器，负责磁盘页面与内存的交换（LRU策略）
// - current_database: 当前使用的数据库名，空字符串表示未选择数据库

#include <QApplication>
#include "mainwindow.h"
#include "buffer_manager.h"
#include "const.h"

// 全局缓冲区管理器实例，所有模块共享同一个
BufferManager buffer_manager;

// 当前使用的数据库名，USE命令会修改此变量
std::string current_database = "";

int main(int argc, char *argv[])
{
    // 创建Qt应用程序实例
    QApplication app(argc, argv);
    // 创建并显示主窗口
    MainWindow window;
    window.show();
    // 进入Qt事件循环，直到窗口关闭
    return app.exec();
}
