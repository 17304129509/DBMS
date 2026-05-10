// interpreter.cpp - SQL解释器
// 负责读取用户输入的SQL语句，进行词法分析和语法分析，
// 然后调用API层执行相应的数据库操作。
//
// 支持的SQL语句：
//   DDL: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX,
//        CREATE DATABASE, DROP DATABASE, ALTER TABLE
//   DML: INSERT, DELETE, SELECT
//   其他: USE, DESCRIBE/DESC, EXIT, EXECFILE

#include "interpreter.h"
#include <fstream>
#include <sstream>

Interpreter::Interpreter() {
}

// 从标准输入读取一条SQL语句（以分号结尾）
// 支持多行输入，直到遇到分号才认为语句结束
void Interpreter::getQuery() {
    std::string tmp;
    do {
        std::cout << ">>> ";
        getline(std::cin, tmp);
        query += tmp;
        query += ' ';
    } while (tmp[tmp.length() - 1] != ';');
    // 将末尾的分号替换为\0，表示字符串结束
    query[query.length() - 2] = '\0';
    // 对SQL语句进行标准化处理
    Normalize();
}

// 对SQL语句进行标准化处理
// 1. 在操作符(*,=,,,(,),<,>)前后添加空格，便于后续按空格分割单词
// 2. 删除连续的多余空格和制表符
// 3. 将第一个SQL关键字转为小写，便于统一匹配
void Interpreter::Normalize() {
    // 在所有操作符前后添加空格以便于分割
    for (int pos = 0; pos < (int)query.length(); pos++) {
        if (query[pos] == '*' || query[pos] == '=' || query[pos] == ',' || query[pos] == '(' || query[pos] == ')' || query[pos] == '<' || query[pos] == '>') {
            if (query[pos - 1] != ' ')
                query.insert(pos++, " ");
            if (query[pos + 1] != ' ')
                query.insert(++pos, " ");
        }
    }
    // 在结尾加一个空格
    query.insert(query.length() - 2, " ");
    // 删除多余的空格
    std::string::iterator it;
    int flag = 0;
    for (it = query.begin(); it < query.end(); it++) {
        if (flag == 0 && (*it == ' ' || *it == '\t')) {
            flag = 1;
            continue;
        }
        if (flag == 1 && (*it == ' ' || *it == '\t')) {
            query.erase(it);
            if (it != query.begin())
                it--;
            continue;
        }
        if (*it != ' ' && *it != '\t') {
            flag = 0;
            continue;
        }
    }
    // 删除开头的空格
    if (query[0] == ' ')
        query.erase(query.begin());
    // 将第一个单词转为小写，用于判断SQL语句类型
    query = getLower(query, 0);
}

// SQL语句执行入口
// 根据第一个关键字判断SQL语句类型，调用对应的EXEC_XXX方法
// 所有异常在此处统一捕获并输出友好的错误信息
void Interpreter::EXEC() {
    try {
        if (query.substr(0, 6) == "select") {
            EXEC_SELECT();
        }
        else if (query.substr(0, 4) == "drop") {
            // DROP语句有三种：DROP TABLE, DROP INDEX, DROP DATABASE
            // 将第二个关键字转为小写后判断
            query = getLower(query, 5);
            if (query.substr(5, 5) == "table")
                EXEC_DROP_TABLE();
            else if (query.substr(5, 5) == "index")
                EXEC_DROP_INDEX();
            else if (query.substr(5, 8) == "database")
                EXEC_DROP_DATABASE();
            else
                throw input_format_error();
        }
        else if (query.substr(0, 6) == "insert") {
            EXEC_INSERT();
        }
        else if (query.substr(0, 6) == "create") {
            // CREATE语句有三种：CREATE TABLE, CREATE INDEX, CREATE DATABASE
            query = getLower(query, 7);
            if (query.substr(7, 5) == "table") {
                EXEC_CREATE_TABLE();
            }
            else if (query.substr(7, 5) == "index") {
                EXEC_CREATE_INDEX();
            }
            else if (query.substr(7, 8) == "database") {
                EXEC_CREATE_DATABASE();
            }
            else
                throw input_format_error();
        }
        else if (query.substr(0, 6) == "delete") {
            EXEC_DELETE();
        }
        else if (query.substr(0, 5) == "alter") {
            // ALTER TABLE <name> ADD/DROP/MODIFY COLUMN ...
            query = getLower(query, 6);
            EXEC_ALTER_TABLE();
        }
        else if (query.substr(0, 3) == "use") {
            // USE <name> 切换当前数据库
            EXEC_USE_DATABASE();
        }
        else if (query.substr(0, 8) == "describe" || query.substr(0, 4) == "desc") {
            // DESCRIBE/DESC <name> 显示表结构
            EXEC_SHOW();
        }
        else if (query.substr(0, 4) == "exit" && query[5] == '\0') {
            EXEC_EXIT();
        }
        else if (query.substr(0, 8) == "execfile") {
            // EXECFILE <path> 从文件中逐行读取并执行SQL语句
            EXEC_FILE();
        }
        else {
            throw input_format_error();
        }
    }

    // ===== 异常处理：输出友好的错误信息 =====
    catch (table_exist error) {
        std::cout << ">>> Error: Table has existed!" << std::endl;
    }
    catch (table_not_exist error) {
        std::cout << ">>> Error: Table not exist!" << std::endl;
    }
    catch (attribute_not_exist error) {
        std::cout << ">>> Error: Attribute not exist!" << std::endl;
    }
    catch (attribute_exist error) {
        std::cout << ">>> Error: Attribute already exists!" << std::endl;
    }
    catch (index_exist error) {
        std::cout << ">>> Error: Index has existed!" << std::endl;
    }
    catch (index_not_exist error) {
        std::cout << ">>> Error: Index not existed!" << std::endl;
    }
    catch (tuple_type_conflict error) {
        std::cout << ">>> Error: Tuple type conflict!" << std::endl;
    }
    catch (primary_key_conflict error) {
        std::cout << ">>> Error: Primary key conflict!" << std::endl;
    }
    catch (data_type_conflict error) {
        std::cout << ">>> Error: data type conflict!" << std::endl;
    }
    catch (index_full error) {
        std::cout << ">>> Error: Index full!" << std::endl;
    }
    catch (unique_conflict error) {
        std::cout << ">>> Error: unique conflict!" << std::endl;
    }
    catch (database_exist error) {
        std::cout << ">>> Error: Database already exists!" << std::endl;
    }
    catch (database_not_exist error) {
        std::cout << ">>> Error: Database not exists!" << std::endl;
    }
    catch (exit_command error) {
        std::cout << ">>> Bye bye~" << std::endl;
        exit(0);
    }
    catch (...) {
        std::cout << ">>> Error: Input format error!" << std::endl;
    }
}

// ===== CREATE DATABASE <name> =====
// 创建一个新的数据库，包含catalog/data/index三个子目录
void Interpreter::EXEC_CREATE_DATABASE() {
    int check_index;
    // "create database " 共16个字符（含空格）
    std::string db_name = getWord(16, check_index);
    // 检查语句是否有多余内容
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    API API;
    API.createDatabase(db_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== DROP DATABASE <name> =====
// 删除一个数据库及其所有数据
void Interpreter::EXEC_DROP_DATABASE() {
    int check_index;
    // "drop database " 共14个字符（含空格）
    std::string db_name = getWord(14, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    API API;
    API.dropDatabase(db_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== USE <name> =====
// 切换当前使用的数据库，后续操作在该数据库的目录下进行
void Interpreter::EXEC_USE_DATABASE() {
    int check_index;
    // "use " 共4个字符（含空格）
    std::string db_name = getWord(4, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    // 验证数据库目录是否存在
    CatalogManager cm;
    if (!cm.hasDatabase(db_name))
        throw database_not_exist();
    // 设置全局变量current_database，影响后续所有文件路径
    current_database = db_name;
    std::cout << ">>> Database changed to " << db_name << std::endl;
}

// ===== ALTER TABLE <name> ADD/DROP/MODIFY COLUMN ... =====
// 修改表结构：添加字段、删除字段、修改字段类型/约束
void Interpreter::EXEC_ALTER_TABLE() {
    API API;
    CatalogManager CM;
    int check_index;

    // "alter table " 共12个字符（含空格），验证TABLE关键字
    if (query.substr(6, 5) != "table")
        throw input_format_error();

    // 提取表名并验证表是否存在
    std::string table_name = getWord(12, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();

    check_index++;
    // 获取操作类型：add/drop/modify
    std::string operation = getWord(check_index, check_index);
    operation = getLower(operation, 0);

    if (operation == "add") {
        // 语法：ALTER TABLE <name> ADD [COLUMN] <attr_name> <type> [unique]
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        // COLUMN关键字可选，如果出现则跳过
        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        // 解析属性类型（int/float/char(n)）
        check_index++;
        short type = getType(check_index, check_index);
        check_index++;

        // 检查是否有unique关键字
        bool unique = false;
        if (check_index + 1 < (int)query.length() && query[check_index + 1] != '\0') {
            std::string unique_check = getWord(check_index + 1, check_index);
            if (getLower(unique_check, 0) == "unique") {
                unique = true;
            }
        }

        API.alterTableAddColumn(table_name, attr_name, type, unique);
        std::cout << ">>> SUCCESS" << std::endl;
    }
    else if (operation == "drop") {
        // 语法：ALTER TABLE <name> DROP COLUMN <attr_name>
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        // COLUMN关键字可选
        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        // 检查语句是否结束
        if (query[check_index + 1] != '\0')
            throw input_format_error();

        API.alterTableDropColumn(table_name, attr_name);
        std::cout << ">>> SUCCESS" << std::endl;
    }
    else if (operation == "modify") {
        // 语法：ALTER TABLE <name> MODIFY COLUMN <attr_name> <type> [unique]
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        // COLUMN关键字可选
        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        // 解析新的属性类型
        check_index++;
        short new_type = getType(check_index, check_index);
        check_index++;

        // 检查是否有unique关键字
        bool unique = false;
        if (check_index + 1 < (int)query.length() && query[check_index + 1] != '\0') {
            std::string unique_check = getWord(check_index + 1, check_index);
            if (getLower(unique_check, 0) == "unique") {
                unique = true;
            }
        }

        API.alterTableModifyColumn(table_name, attr_name, new_type, unique);
        std::cout << ">>> SUCCESS" << std::endl;
    }
    else {
        throw input_format_error();
    }
}

// ===== CREATE INDEX <index_name> ON <table_name>(<attr_name>) =====
void Interpreter::EXEC_CREATE_INDEX() {
    CatalogManager CM;
    API API;
    std::string index_name;
    std::string table_name;
    std::string attr_name;
    int check_index;
    // "create index " 共13个字符
    index_name = getWord(13, check_index);
    check_index++;
    // 验证ON关键字
    if (getLower(query, check_index).substr(check_index, 2) != "on")
        throw input_format_error();
    table_name = getWord(check_index + 3, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();
    // 解析括号中的属性名
    if (query[check_index + 1] != '(')
        throw input_format_error();
    attr_name = getWord(check_index + 3, check_index);
    if (query[check_index + 1] != ')' || query[check_index + 3] != '\0')
        throw input_format_error();
    API.createIndex(table_name, index_name, attr_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== DROP INDEX <index_name> ON <table_name> =====
void Interpreter::EXEC_DROP_INDEX() {
    API API;
    std::string table_name;
    std::string index_name;
    int check_index;
    // "drop index " 共11个字符
    index_name = getWord(11, check_index);
    check_index++;
    // 验证ON关键字
    if (getLower(query, check_index).substr(check_index, 2) != "on")
        throw input_format_error();
    table_name = getWord(check_index + 3, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    API.dropIndex(table_name, index_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== EXIT =====
// 退出MiniSQL程序
void Interpreter::EXEC_EXIT() {
    throw exit_command();
}

// ===== EXECFILE <file_path> =====
// 从指定文件中逐行读取SQL语句并执行
void Interpreter::EXEC_FILE() {
    int check_index = 0;
    int start_index = 0;
    std::string tmp_query;
    // "execfile " 共9个字符
    std::string file_path = getWord(9, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    // 读取整个文件内容
    std::fstream fs(file_path);
    std::stringstream ss;
    ss << fs.rdbuf();
    tmp_query = ss.str();
    // 按行分割并逐条执行
    check_index = 0;
    do {
        while (tmp_query[check_index] != '\n')
            check_index++;
        query = tmp_query.substr(start_index, check_index - start_index);
        check_index++;
        start_index = check_index;
        Normalize();
        EXEC();
    } while (tmp_query[check_index] != '\0');
}

// ===== DESCRIBE/DESC <table_name> =====
// 显示表的结构信息（属性、类型、索引等）
void Interpreter::EXEC_SHOW() {
    CatalogManager CM;
    std::string table_name;
    int check_index;
    // 跳过describe/desc关键字，提取表名
    getWord(0, check_index);
    table_name = getWord(check_index + 1, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    CM.showTable(table_name);
}

// ===== DELETE FROM <table_name> [WHERE <attr> <op> <value>] =====
void Interpreter::EXEC_DELETE() {
    API API;
    CatalogManager CM;
    Where where_delete;
    int check_index;
    std::string table_name;
    std::string attr_name;
    std::string relation;
    // 验证FROM关键字
    if (getLower(query, 7).substr(7, 4) != "from")
        throw input_format_error();
    // "delete from " 共12个字符
    table_name = getWord(12, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();

    // 没有WHERE条件时，删除表中所有记录
    if (query[check_index + 1] == '\0') {
        attr_name = "";
        API.deleteRecord(table_name, attr_name, where_delete);
        std::cout << ">>> SUCCESS" << std::endl;
        return;
    }

    // 解析WHERE条件
    if (getLower(query, check_index + 1).substr(check_index + 1, 5) != "where")
        throw input_format_error();
    // 提取属性名
    attr_name = getWord(check_index + 7, check_index);
    if (!CM.hasAttribute(table_name, attr_name))
        throw attribute_not_exist();
    // 提取关系运算符
    relation = getRelation(check_index + 1, check_index);
    // 将关系运算符字符串转为枚举值
    if (relation == "<")
        where_delete.relation_character = LESS;
    else if (relation == "< =")
        where_delete.relation_character = LESS_OR_EQUAL;
    else if (relation == "=")
        where_delete.relation_character = EQUAL;
    else if (relation == "> =")
        where_delete.relation_character = GREATER_OR_EQUAL;
    else if (relation == ">")
        where_delete.relation_character = GREATER;
    else if (relation == "! =")
        where_delete.relation_character = NOT_EQUAL;
    else
        throw input_format_error();
    // 提取比较值
    std::string value_delete = getWord(check_index + 1, check_index);

    // 根据属性类型解析比较值
    Attribute tmp_attr = CM.getAttribute(table_name);
    for (int i = 0; i < tmp_attr.num; i++)
    {
        if (attr_name == tmp_attr.name[i]) {
            where_delete.data.type = tmp_attr.type[i];
            switch (where_delete.data.type) {
            case -1:
                // int类型：将字符串转为整数
                try {
                    where_delete.data.datai = stringToNum<int>(value_delete);
                }
                catch (...) {
                    throw data_type_conflict();
                }
                break;
            case 0:
                // float类型：将字符串转为浮点数
                try {
                    where_delete.data.dataf = stringToNum<float>(value_delete);
                }
                catch (...) {
                    throw data_type_conflict();
                }
                break;
            default:
                // string类型：需要用引号包围
                try {
                    if (!(value_delete[0] == '\'' && value_delete[value_delete.length() - 1] == '\'') && !(value_delete[0] == '"' && value_delete[value_delete.length() - 1] == '"'))
                        throw input_format_error();
                    // 去掉首尾引号
                    where_delete.data.datas = value_delete.substr(1, value_delete.length() - 2);
                }
                catch (...) {
                    throw data_type_conflict();
                }
                break;
            }
            break;
        }
    }
    API.deleteRecord(table_name, attr_name, where_delete);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== INSERT INTO <table_name> VALUES (<value1>, <value2>, ...) =====
void Interpreter::EXEC_INSERT() {
    API API;
    CatalogManager CM;
    std::string table_name;
    int check_index;
    Tuple tuple_insert;
    Attribute attr_exist;
    // 验证INTO关键字
    if (getLower(query, 7).substr(7, 4) != "into")
        throw input_format_error();
    // "insert into " 共12个字符
    table_name = getWord(12, check_index);
    // 验证VALUES关键字
    if (getLower(query, check_index + 1).substr(check_index + 1, 6) != "values")
        throw input_format_error();
    check_index += 8;
    // 验证左括号
    if (query[check_index] != '(')
        throw input_format_error();
    if (!CM.hasTable(table_name))
        throw table_not_exist();
    // 获取表的属性信息，用于类型检查
    attr_exist = CM.getAttribute(table_name);
    check_index--;
    int num_of_insert = 0;
    // 逐个解析括号中的值
    while (query[check_index + 1] != '\0' && query[check_index + 1] != ')') {
        if (num_of_insert >= attr_exist.num)
            throw input_format_error();
        check_index += 3;
        std::string value_insert = getWord(check_index, check_index);
        Data insert_data;
        insert_data.type = attr_exist.type[num_of_insert];
        // 根据属性类型解析插入值
        switch (attr_exist.type[num_of_insert]) {
        case -1:
            // int类型
            try {
                insert_data.datai = stringToNum<int>(value_insert);
            }
            catch (...) {
                throw data_type_conflict();
            }
            break;
        case 0:
            // float类型
            try {
                insert_data.dataf = stringToNum<float>(value_insert);
            }
            catch (...) {
                throw data_type_conflict();
            }
            break;
        default:
            // string类型：需要用引号包围，且长度不能超过定义的char长度
            try {
                if (!(value_insert[0] == '\'' && value_insert[value_insert.length() - 1] == '\'') && !(value_insert[0] == '"' && value_insert[value_insert.length() - 1] == '"'))
                    throw input_format_error();
                if (value_insert.length() - 1 > attr_exist.type[num_of_insert])
                    throw input_format_error();
                // 去掉首尾引号
                insert_data.datas = value_insert.substr(1, value_insert.length() - 2);
            }
            catch (input_format_error error) {
                throw input_format_error();
            }
            catch (...) {
                throw data_type_conflict();
            }
            break;
        }
        tuple_insert.addData(insert_data);
        num_of_insert++;
    }
    // 验证右括号和插入值的数量
    if (query[check_index + 1] == '\0')
        throw input_format_error();
    if (num_of_insert != attr_exist.num)
        throw input_format_error();
    API.insertRecord(table_name, tuple_insert);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== SELECT <attr_list|*> FROM <table_name> [WHERE ... [AND|OR ...]] =====
void Interpreter::EXEC_SELECT() {
    API API;
    CatalogManager CM;
    std::string table_name;
    std::vector<std::string> attr_name;
    std::vector<std::string> target_name;
    std::vector<Where> where_select;
    std::string tmp_target_name;
    std::string tmp_value;
    Where tmp_where;
    std::string relation;
    Table output_table;
    // op: 0=OR, 1=AND，默认为OR（单条件时无影响）
    char op = 0;
    int check_index;
    int flag = 0;
    // 解析SELECT后面的属性列表或*
    if (getWord(7, check_index) == "*")
    {
        // SELECT * 表示查询所有属性
        flag = 1;
        check_index++;
    }
    else {
        // SELECT attr1, attr2, ... 解析逗号分隔的属性名列表
        check_index = 7;
        while (1) {
            attr_name.push_back(getWord(check_index, check_index));
            if (query[++check_index] != ',')
                break;
            else
                check_index += 2;
        }
    }
    // 验证FROM关键字
    if (getLower(query, check_index).substr(check_index, 4) != "from")
        throw input_format_error();
    check_index += 5;
    table_name = getWord(check_index, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();
    Attribute tmp_attr = CM.getAttribute(table_name);
    // 验证所有查询的属性是否存在
    if (!flag) {
        for (int index = 0; index < (int)attr_name.size(); index++) {
            if (!CM.hasAttribute(table_name, attr_name[index]))
                throw attribute_not_exist();
        }
    }
    else {
        // SELECT * 时，将所有属性名加入列表
        for (int index = 0; index < tmp_attr.num; index++) {
            attr_name.push_back(tmp_attr.name[index]);
        }
    }
    check_index++;
    if (query[check_index] == '\0')
        // 没有WHERE条件，返回全表
        output_table = API.selectRecord(table_name, target_name, where_select, op);
    else {
        // 解析WHERE条件
        if (getLower(query, check_index).substr(check_index, 5) != "where")
            throw input_format_error();
        check_index += 6;
        // 循环解析多个WHERE条件（用AND/OR连接）
        while (1) {
            // 提取条件中的属性名
            tmp_target_name = getWord(check_index, check_index);
            if (!CM.hasAttribute(table_name, tmp_target_name))
                throw attribute_not_exist();
            target_name.push_back(tmp_target_name);
            // 提取关系运算符
            relation = getRelation(check_index + 1, check_index);
            // 转换关系运算符为枚举值
            if (relation == "<")
                tmp_where.relation_character = LESS;
            else if (relation == "< =")
                tmp_where.relation_character = LESS_OR_EQUAL;
            else if (relation == "=")
                tmp_where.relation_character = EQUAL;
            else if (relation == "> =")
                tmp_where.relation_character = GREATER_OR_EQUAL;
            else if (relation == ">")
                tmp_where.relation_character = GREATER;
            else if (relation == "! =")
                tmp_where.relation_character = NOT_EQUAL;
            else
                throw input_format_error();
            // 提取比较值
            tmp_value = getWord(check_index + 1, check_index);
            // 根据属性类型解析比较值
            for (int i = 0; i < tmp_attr.num; i++)
            {
                if (tmp_target_name == tmp_attr.name[i]) {
                    tmp_where.data.type = tmp_attr.type[i];
                    switch (tmp_where.data.type) {
                    case -1:
                        try {
                            tmp_where.data.datai = stringToNum<int>(tmp_value);
                        }
                        catch (...) {
                            throw data_type_conflict();
                        }
                        break;
                    case 0:
                        try {
                            tmp_where.data.dataf = stringToNum<float>(tmp_value);
                        }
                        catch (...) {
                            throw data_type_conflict();
                        }
                        break;
                    default:
                        try {
                            if (!(tmp_value[0] == '\'' && tmp_value[tmp_value.length() - 1] == '\'') && !(tmp_value[0] == '"' && tmp_value[tmp_value.length() - 1] == '"'))
                                throw input_format_error();
                            tmp_where.data.datas = tmp_value.substr(1, tmp_value.length() - 2);
                        }
                        catch (input_format_error error) {
                            throw input_format_error();
                        }
                        catch (...) {
                            throw data_type_conflict();
                        }
                    }
                    break;
                }
            }

            where_select.push_back(tmp_where);
            // 检查是否还有AND/OR连接的条件
            if (query[check_index + 1] == '\0')
                break;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 3) == "and")
                op = 1;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 2) == "or")
                op = 0;
            else
                throw input_format_error();
            // 跳过AND/OR关键字
            getWord(check_index + 1, check_index);
            check_index++;
        }

        output_table = API.selectRecord(table_name, target_name, where_select, op);
    }

    // ===== 格式化输出查询结果 =====
    Attribute attr_record = output_table.attr_;
    // use数组记录每个输出列在属性表中的实际索引位置
    int use[32] = { 0 };
    if (attr_name.size() == 0) {
        for (int i = 0; i < attr_record.num; i++)
            use[i] = i;
    }
    else {
        for (int i = 0; i < (int)attr_name.size(); i++)
            for (int j = 0; j < attr_record.num; j++) {
                if (attr_record.name[j] == attr_name[i])
                {
                    use[i] = j;
                    break;
                }
            }
    }
    // 计算每列的最大宽度用于对齐
    std::vector<Tuple> output_tuple = output_table.getTuple();
    int longest = -1;
    for (int index = 0; index < (int)attr_name.size(); index++) {
        if ((int)attr_record.name[use[index]].length() > longest)
            longest = (int)attr_record.name[use[index]].length();
    }
    for (int index = 0; index < (int)attr_name.size(); index++) {
        int type = attr_record.type[use[index]];
        if (type == -1) {
            for (int i = 0; i < (int)output_tuple.size(); i++) {
                if (longest < getBits(output_tuple[i].getData()[use[index]].datai)) {
                    longest = getBits(output_tuple[i].getData()[use[index]].datai);
                }
            }
        }
        if (type == 0) {
            for (int i = 0; i < (int)output_tuple.size(); i++) {
                if (longest < getBits(output_tuple[i].getData()[use[index]].dataf)) {
                    longest = getBits(output_tuple[i].getData()[use[index]].dataf);
                }
            }
        }
        if (type > 0) {
            for (int i = 0; i < (int)output_tuple.size(); i++) {
                if (longest < (int)output_tuple[i].getData()[use[index]].datas.length()) {
                    longest = (int)output_tuple[i].getData()[use[index]].datas.length();
                }
            }
        }
    }
    longest += 1;
    // 输出表头
    for (int index = 0; index < (int)attr_name.size(); index++) {
        if (index != (int)attr_name.size() - 1) {
            for (int i = 0; i < (longest - (int)attr_record.name[use[index]].length()) / 2; i++)
                printf(" ");
            printf("%s", attr_record.name[use[index]].c_str());
            for (int i = 0; i < longest - (longest - (int)attr_record.name[use[index]].length()) / 2 - (int)attr_record.name[use[index]].length(); i++)
                printf(" ");
            printf("|");
        }
        else {
            for (int i = 0; i < (longest - (int)attr_record.name[use[index]].length()) / 2; i++)
                printf(" ");
            printf("%s", attr_record.name[use[index]].c_str());
            for (int i = 0; i < longest - (longest - (int)attr_record.name[use[index]].length()) / 2 - (int)attr_record.name[use[index]].length(); i++)
                printf(" ");
            printf("\n");
        }
    }
    // 输出分隔线
    for (int index = 0; index < (int)attr_name.size() * (longest + 1); index++) {
        std::cout << "-";
    }
    std::cout << std::endl;
    // 输出每一行数据
    for (int index = 0; index < (int)output_tuple.size(); index++) {
        for (int i = 0; i < (int)attr_name.size(); i++)
        {
            switch (output_tuple[index].getData()[use[i]].type) {
            case -1:
                if (i != (int)attr_name.size() - 1) {
                    int len = output_tuple[index].getData()[use[i]].datai;
                    len = getBits(len);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        printf(" ");
                    printf("%d", output_tuple[index].getData()[use[i]].datai);
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        printf(" ");
                    printf("|");
                }
                else {
                    int len = output_tuple[index].getData()[use[i]].datai;
                    len = getBits(len);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        printf(" ");
                    printf("%d", output_tuple[index].getData()[use[i]].datai);
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        printf(" ");
                    printf("\n");
                }
                break;
            case 0:
                if (i != (int)attr_name.size() - 1) {
                    float num = output_tuple[index].getData()[use[i]].dataf;
                    int len = getBits(num);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        printf(" ");
                    printf("%.2f", output_tuple[index].getData()[use[i]].dataf);
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        printf(" ");
                    printf("|");
                }
                else {
                    float num = output_tuple[index].getData()[use[i]].dataf;
                    int len = getBits(num);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        printf(" ");
                    printf("%.2f", output_tuple[index].getData()[use[i]].dataf);
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        printf(" ");
                    printf("\n");
                }
                break;
            default:
                std::string tmp = output_tuple[index].getData()[use[i]].datas;
                if (i != (int)attr_name.size() - 1) {
                    for (int i = 0; i < (longest - (int)tmp.length()) / 2; i++)
                        printf(" ");
                    printf("%s", tmp.c_str());
                    for (int i = 0; i < longest - (longest - (int)tmp.length()) / 2 - (int)tmp.length(); i++)
                        printf(" ");
                    printf("|");
                }
                else {
                    for (int i = 0; i < (longest - (int)tmp.length()) / 2; i++)
                        printf(" ");
                    printf("%s", tmp.c_str());
                    for (int i = 0; i < longest - (longest - (int)tmp.length()) / 2 - (int)tmp.length(); i++)
                        printf(" ");
                    printf("\n");
                }
                break;
            }
        }
    }
}

// ===== CREATE TABLE <name> (<attr1> <type1> [unique], ..., PRIMARY KEY(<attr>)) =====
void Interpreter::EXEC_CREATE_TABLE() {
    std::string table_name;
    int check_index;
    // "create table " 共13个字符
    table_name = getWord(13, check_index);
    Index index_create;
    index_create.num = 0;
    Attribute attr_create;
    std::string attr_name;
    int primary = -1;
    int attr_num = 0;
    // 循环解析括号中的属性定义
    while (1) {
        check_index += 3;
        if (query[check_index] == '\0') {
            if (query[check_index - 2] == '\0')
                throw input_format_error();
            else
                break;
        }
        attr_name = getWord(check_index, check_index);
        std::string check_primary(attr_name);
        check_primary = getLower(check_primary, 0);
        // 检查是否是PRIMARY KEY定义
        if (check_primary == "primary") {
            int tmp_end = check_index;
            std::string check_key = getWord(tmp_end + 1, tmp_end);
            // 将KEY关键字转为小写后再比较，支持KEY/Key/key等写法
            check_key = getLower(check_key, 0);
            if (check_key != "key") {
                attr_create.name[attr_num] = attr_name;
                break;
            }
            else {
                // 解析PRIMARY KEY(attr_name)
                check_index = tmp_end + 3;
                std::string unique_name = getWord(check_index, check_index);
                int hasset = 1;
                for (int find_name = 0; find_name < attr_create.num; find_name++) {
                    if (attr_create.name[find_name] == unique_name) {
                        hasset = 0;
                        primary = find_name;
                        attr_create.unique[find_name] = true;
                        check_index += 2;
                        break;
                    }
                }
                if (hasset)
                    throw input_format_error();
                continue;
            }
        }
        else
            attr_create.name[attr_num] = attr_name;
        // 解析属性类型
        check_index++;
        attr_create.type[attr_num] = getType(check_index, check_index);
        attr_create.unique[attr_num] = false;
        // 检查是否有unique关键字
        if (query[check_index + 1] == 'u' || query[check_index + 1] == 'U') {
            query = getLower(query, 0);
            if (getWord(check_index + 1, check_index) == "unique") {
                attr_create.unique[attr_num] = true;
            }
            else
                throw input_format_error();
        }
        attr_num++;
        attr_create.num = attr_num;
    }
    API API;
    API.createTable(table_name, attr_create, primary, index_create);
    std::cout << ">>> SUCCESS" << std::endl;
}

// ===== DROP TABLE <name> =====
void Interpreter::EXEC_DROP_TABLE() {
    API API;
    std::string table_name;
    int check_index;
    // "drop table " 共11个字符
    table_name = getWord(11, check_index);
    if (query[check_index + 1] != '\0')
        throw input_format_error();
    API.dropTable(table_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

// 从query中解析属性类型
// 返回值：-1=int, 0=float, >0=char(n)+1（char类型存储时+1以区分float的0）
// 对于char类型，还需要跳过括号中的长度参数
short Interpreter::getType(int pos, int& end_pos) {
    std::string type = getWord(pos, end_pos);
    // 将类型关键字统一转为小写，支持大小写无关匹配
    for (int i = 0; i < (int)type.length(); i++)
        if (type[i] >= 'A' && type[i] <= 'Z')
            type[i] += 32;

    if (type == "int")
        return -1;
    else if (type == "float")
        return 0;
    else if (type == "char" || type == "varchar") {
        // char(n)和varchar(n)类型：跳过左括号，读取长度，跳过右括号
        end_pos += 3;
        std::string length = getWord(end_pos, end_pos);
        end_pos += 2;
        // 返回长度+1，因为0已被float占用，需要+1来区分
        return atoi(length.c_str()) + 1;
    }
    throw input_format_error();
}

// 从query的指定位置提取一个单词（以空格或\0为分隔符）
// end_pos返回单词结束位置
std::string Interpreter::getWord(int pos, int& end_pos) {
    std::string PartWord = "";
    for (int pos1 = pos; pos1 < (int)query.length(); pos1++) {
        if (query[pos1] == ' ' || query[pos1] == '\0')
        {
            PartWord = query.substr(pos, pos1 - pos);
            end_pos = pos1;
            return PartWord;
        }
    }
    return PartWord;
}

// 将字符串中从指定位置开始到空格/结尾的单词转为小写
// 用于SQL关键字的大小写无关匹配
std::string Interpreter::getLower(std::string str, int pos) {
    for (int index = pos;; index++) {
        if (str[index] == ' ' || str[index] == '\0')
            break;
        else if (str[index] >= 'A' && str[index] <= 'Z')
            str[index] += 32;
    }
    return str;
}

// 从query中提取关系运算符（<, <=, =, >=, >, !=）
// 注意：经过Normalize处理后，<=变成< =，>=变成> =，!=变成! =
std::string Interpreter::getRelation(int pos, int& end_pos) {
    std::string PartWord = "";
    for (int pos1 = pos; pos1 < (int)query.length(); pos1++) {
        if (query[pos1] == ' ')
            continue;
        if (query[pos1] != '<' && query[pos1] != '>' && query[pos1] != '=' && query[pos1] != '!')
        {
            PartWord = query.substr(pos, pos1 - pos - 1);
            end_pos = pos1 - 1;
            return PartWord;
        }
    }
    return PartWord;
}

// 计算整数的显示位数（用于SELECT结果对齐）
int Interpreter::getBits(int num) {
    int bit = 0;
    if (num == 0)
        return 1;
    if (num < 0) {
        bit++;
        num = -num;
    }
    while (num != 0) {
        num /= 10;
        bit++;
    }
    return bit;
}

// 计算浮点数的显示位数（整数部分+3位小数部分）
int Interpreter::getBits(float num) {
    int bit = 0;
    if ((int)num == 0)
        return 4;
    if (num < 0) {
        bit++;
        num = -num;
    }
    int integer_part = (int)num;
    while (integer_part != 0) {
        bit++;
        integer_part /= 10;
    }
    return bit + 3;
}
