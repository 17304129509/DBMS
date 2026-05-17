#ifndef _EXCEPTION_H_
#define _EXCEPTION_H_ 1

#include <exception>

// ===== 表相关异常 =====

// 创建表时表名已存在
class table_exist : public std::exception {

};

// 操作的表不存在
class table_not_exist : public std::exception {

};

// ===== 属性相关异常 =====

// 操作的属性不存在（用于WHERE条件、ALTER TABLE DROP/MODIFY等）
class attribute_not_exist : public std::exception {

};

// 添加的属性已存在（用于ALTER TABLE ADD时属性名重复）
class attribute_exist : public std::exception {

};

// ===== 索引相关异常 =====

// 创建索引时索引已存在
class index_exist : public std::exception {

};

// 删除索引时索引不存在
class index_not_exist : public std::exception {

};

// 索引数量已满（每张表最多10个索引）
class index_full : public std::exception {

};

// ===== 数据相关异常 =====

// 插入元组的类型与表定义的属性类型不匹配
class tuple_type_conflict : public std::exception {

};

// 插入记录时主键冲突
class primary_key_conflict : public std::exception {

};

// WHERE条件中的数据类型与属性类型不匹配
class data_type_conflict : public std::exception {

};

// 插入记录时unique属性值冲突
class unique_conflict : public std::exception {

};

// ===== 数据库相关异常 =====

// 创建数据库时数据库名已存在
class database_exist : public std::exception {

};

// 操作的数据库不存在（DROP DATABASE、USE等）
class database_not_exist : public std::exception {

};

// ===== 其他异常 =====

// SQL语句格式错误
class input_format_error : public std::exception {

};

// 用户输入exit命令，用于退出程序
class exit_command : public std::exception {

};

#endif
