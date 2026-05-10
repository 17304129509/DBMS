

#ifndef _INTERPRETER_H_
#define _INTERPRETER_H_ 1

#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include "basicType.h"
#include "api.h"
#include "exception.h"
#include "const.h"
#include "template_function.h"

class Interpreter {
public:
    Interpreter();
    // 输入：void
    // 输出：void
    // 功能：从标准输入获取SQL查询
    void getQuery();
    // 输入：void
    // 输出：void
    // 功能：执行SQL查询
    void EXEC();

private:
    std::string query;
    // 输入：字符串，开始位置
    // 输出：小写化后的字符串
    // 功能：将字符串中从指定位置开始的单词转为小写
    std::string getLower(std::string str, int start);
    // 输入：开始位置，结束位置（引用传出）
    // 输出：提取的单词
    // 功能：从query中提取一个单词
    std::string getWord(int start, int& end);
    // 输入：void
    // 输出：标准化后的字符串
    // 功能：对SQL语句进行标准化处理（添加空格、小写化关键字）
    void Normalize();
    // 输入：开始位置，结束位置（引用传出）
    // 输出：属性类型（-1=int, 0=float, >0=char长度）
    // 功能：从query中解析属性类型
    short getType(int pos, int& end_pos);
    // 输入：开始位置，结束位置（引用传出）
    // 输出：关系运算符字符串
    // 功能：从query中提取关系运算符
    std::string getRelation(int pos, int& end_pos);
    // 输入：整数
    // 输出：该整数的位数
    int getBits(int num);
    // 输入：浮点数
    // 输出：该浮点数的显示位数
    int getBits(float num);

    // ===== 原有的SQL命令执行方法 =====
    void EXEC_SELECT();
    void EXEC_DROP_TABLE();
    void EXEC_DROP_INDEX();
    void EXEC_CREATE_TABLE();
    void EXEC_CREATE_INDEX();
    void EXEC_INSERT();
    void EXEC_DELETE();
    void EXEC_SHOW();
    void EXEC_EXIT();
    void EXEC_FILE();

    // ===== 新增的SQL命令执行方法 =====
    // CREATE DATABASE <name>
    void EXEC_CREATE_DATABASE();
    // DROP DATABASE <name>
    void EXEC_DROP_DATABASE();
    // USE <name>
    void EXEC_USE_DATABASE();
    // ALTER TABLE <name> ADD/DROP/MODIFY COLUMN ...
    void EXEC_ALTER_TABLE();
};

#endif
