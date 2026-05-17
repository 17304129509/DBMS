

#ifndef _API_H_
#define _API_H_ 1

#include <string>
#include <vector>
#include "basicType.h"
#include "catalog_manager.h"
#include "record_manager.h"
#include "index_manager.h"
#include "exception.h"
#include "const.h"

class API {
public:
    // 输入：表名，属性对象，主键编号，索引对象
    // 输出：bool
    // 功能：创建表
    // 异常：如果表已经存在，抛出table_exist异常
    bool createTable(std::string table_name, Attribute attr, int primary, Index index);
    // 输入：表名
    // 输出：bool
    // 功能：删除表
    // 异常：如果表不存在，抛出table_not_exist异常
    bool dropTable(std::string table_name);
    // 输入：表名，属性名，索引名
    // 输出：bool
    // 功能：在对应表的对应属性上建立索引
    // 异常：如果表不存在，抛出table_not_exist异常。如果属性不存在，抛出attribute_not_exist异常。
    // 如果索引已经存在，抛出index_exist异常。
    bool createIndex(std::string table_name, std::string attr_name, std::string index_name);
    // 输入：表名，索引名
    // 输出：bool
    // 功能：删除对应表的对应属性上的索引
    // 异常：如果表不存在，抛出table_not_exist异常。如果索引不存在，抛出index_not_exist异常。
    bool dropIndex(std::string table_name, std::string index_name);
    // 输入：表名，一个元组
    // 输出：bool
    // 功能：向对应表中插入一条记录
    // 异常：如果表不存在，抛出table_not_exist异常
    bool insertRecord(std::string table_name, Tuple& tuple);
    // 输入：表名
    // 输出：int(删除的记录数)
    // 功能：删除对应表中所有记录
    // 异常：如果表不存在，抛出table_not_exist异常
    int deleteRecord(std::string table_name);
    // 输入：表名，目标属性，一个Where类型的对象
    // 输出：int(删除的记录数)
    // 功能：删除对应表中所有目标属性值满足Where条件的记录
    // 异常：如果表不存在，抛出table_not_exist异常。如果属性不存在，抛出attribute_not_exist异常。
    int deleteRecord(std::string table_name, std::string target_attr, Where where);
    int updateRecord(std::string table_name, std::vector<std::string> attr_names, std::vector<Data> values, std::vector<std::string> target_name, std::vector<Where> where, char op);
    // 输入：表名
    // 输出：Table类型对象
    // 功能：返回整张表
    // 异常：如果表不存在，抛出table_not_exist异常
    Table selectRecord(std::string table_name);
    // 输入：表名，目标属性，一个Where类型的对象
    // 输出：Table类型对象
    // 功能：返回包含所有目标属性满足Where条件的记录的表
    // 异常：如果表不存在，抛出table_not_exist异常。如果属性不存在，抛出attribute_not_exist异常。
    Table selectRecord(std::string table_name, std::string target_attr, Where where);
    // 输入：表名，目标属性名列表，Where条件列表，AND/OR操作符
    // 输出：Table类型对象
    // 功能：多条件查询，支持AND/OR组合
    Table selectRecord(std::string table_name, std::vector<std::string> target_name, std::vector<Where> where, char op);
    // 输入：表名
    // 输出：void
    // 功能：显示表的信息
    // 异常：如果表不存在，抛出table_not_exist异常
    void showTable(std::string table_name);

    // ===== 以下为新增的数据库相关方法 =====

    // 输入：数据库名
    // 输出：bool
    // 功能：创建数据库（创建目录结构和catalog文件）
    // 异常：如果数据库已存在，抛出database_exist异常
    bool createDatabase(std::string db_name);
    // 输入：数据库名
    // 输出：bool
    // 功能：删除数据库（删除目录和所有数据）
    // 异常：如果数据库不存在，抛出database_not_exist异常
    bool dropDatabase(std::string db_name);

    // ===== 以下为新增的ALTER TABLE相关方法 =====

    // 输入：表名，属性名，类型，是否唯一
    // 输出：bool
    // 功能：为表添加一个新字段
    bool alterTableAddColumn(std::string table_name, std::string attr_name, short type, bool unique);
    // 输入：表名，属性名
    // 输出：bool
    // 功能：删除表中的一个字段
    bool alterTableDropColumn(std::string table_name, std::string attr_name);
    // 输入：表名，属性名，新类型，是否唯一
    // 输出：bool
    // 功能：修改表中一个字段的类型和约束
    bool alterTableModifyColumn(std::string table_name, std::string attr_name, short new_type, bool new_unique);

private:
    // 根据current_database返回索引文件的完整路径
    std::string getIndexFilePath(std::string file_path);
    // 检查一条记录是否满足单个WHERE条件（用于多条件查询的内存过滤）
    bool checkWhereCondition(std::vector<Data>& data, Attribute& attr, std::string target_attr, Where& where);
    // 重建表的所有索引（ALTER TABLE后调用）
    void rebuildIndexes(std::string table_name);
};

#endif
