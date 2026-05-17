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
#include "log_manager.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <io.h>

Interpreter::Interpreter() {
}

static std::string trimTrailing(const std::string& s) {
    int end = (int)s.length() - 1;
    while (end >= 0 && (s[end] == ' ' || s[end] == '\t' || s[end] == ';' || s[end] == '\n' || s[end] == '\r'))
        end--;
    return s.substr(0, end + 1);
}

std::string Interpreter::executeQuery(const std::string& sql) {
    query = sql;
    if (query.empty()) return "";
    for (int i = 0; i < (int)query.length(); i++) {
        if (query[i] == '\n' || query[i] == '\r')
            query[i] = ' ';
    }
    query = trimTrailing(query);
    if (query.empty()) return "";
    query += ' ';
    Normalize();

    std::string lower_q = query;
    for (auto& c : lower_q) c = tolower(c);
    if (lower_q.substr(0, 4) == "exit" && query[4] == ' ') {
        return "Bye bye~";
    }

    std::stringstream ss;
    std::streambuf* old_cout = std::cout.rdbuf(ss.rdbuf());

    try {
        EXEC();
    } catch (...) {
        std::cout.rdbuf(old_cout);
        throw;
    }

    std::cout.rdbuf(old_cout);

    std::string result = ss.str();
    while (!result.empty() && (result.back() == '\n' || result.back() == '\r'))
        result.pop_back();
    return result;
}

void Interpreter::getQuery() {
    std::string tmp;
    do {
        std::cout << ">>> ";
        getline(std::cin, tmp);
        query += tmp;
        query += ' ';
    } while (tmp[tmp.length() - 1] != ';');
    query = trimTrailing(query);
    if (!query.empty())
        query += ' ';
    Normalize();
}

// 对SQL语句进行标准化处理
// 1. 在操作符(*,=,,,(,),<,>)前后添加空格，便于后续按空格分割单词
// 2. 删除连续的多余空格和制表符
// 3. 将第一个SQL关键字转为小写，便于统一匹配
void Interpreter::Normalize() {
    for (int pos = 0; pos < (int)query.length(); pos++) {
        if (query[pos] == '*' || query[pos] == '=' || query[pos] == ',' || query[pos] == '(' || query[pos] == ')' || query[pos] == '<' || query[pos] == '>') {
            if (pos > 0 && query[pos - 1] != ' ')
                query.insert(pos++, " ");
            if (pos + 1 < (int)query.length() && query[pos + 1] != ' ')
                query.insert(++pos, " ");
        }
    }
    if (query.empty() || query.back() != ' ')
        query += ' ';
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
    if (!query.empty() && query[0] == ' ')
        query.erase(query.begin());
    query = getLower(query, 0);
}

bool Interpreter::isEnd(int pos) {
    return pos >= (int)query.length();
}

void Interpreter::EXEC() {
    std::string log_query = query;
    LogManager::getInstance().log("SQL", log_query);
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
            else if (query.substr(7, 4) == "view") {
                EXEC_CREATE_VIEW();
            }
            else
                throw input_format_error();
        }
        else if (query.substr(0, 6) == "delete") {
            EXEC_DELETE();
        }
        else if (query.substr(0, 6) == "update") {
            EXEC_UPDATE();
        }
        else if (query.substr(0, 5) == "alter") {
            query = getLower(query, 6);
            EXEC_ALTER_TABLE();
        }
        else if (query.substr(0, 3) == "use") {
            EXEC_USE_DATABASE();
        }
        else if (query.substr(0, 8) == "describe" || query.substr(0, 4) == "desc") {
            // DESCRIBE/DESC <name> 显示表结构
            EXEC_SHOW();
        }
        else if (query.substr(0, 4) == "show") {
            EXEC_SHOW_DATABASES();
        }
        else if (query.substr(0, 4) == "exit" && query[4] == ' ') {
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
    catch (const std::exception& e) {
        std::cout << ">>> Error: " << e.what() << std::endl;
    }
    catch (...) {
        std::cout << ">>> Error: Input format error!" << std::endl;
    }
}

void Interpreter::EXEC_CREATE_VIEW() {
    int check_index;
    std::string view_name = getWord(12, check_index);
    if (getLower(query, check_index + 1).substr(check_index + 1, 2) != "as")
        throw input_format_error();
    std::string select_stmt = query.substr(check_index + 4);
    std::string view_path;
    if (current_database.empty()) {
        view_path = "./database/catalog/" + view_name + ".view";
    } else {
        view_path = "./database/" + current_database + "/catalog/" + view_name + ".view";
    }
    std::ifstream check(view_path);
    if (check.is_open()) {
        check.close();
        std::cout << ">>> Error: View already exists!" << std::endl;
        return;
    }
    std::ofstream ofs(view_path);
    ofs << select_stmt;
    ofs.close();
    std::cout << ">>> SUCCESS" << std::endl;
}

void Interpreter::EXEC_CREATE_DATABASE() {
    int check_index;
    std::string db_name = getWord(16, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    API API;
    API.createDatabase(db_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

void Interpreter::EXEC_DROP_DATABASE() {
    int check_index;
    std::string db_name = getWord(14, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    API API;
    API.dropDatabase(db_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

void Interpreter::EXEC_USE_DATABASE() {
    int check_index;
    std::string db_name = getWord(4, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    CatalogManager cm;
    if (!cm.hasDatabase(db_name))
        throw database_not_exist();
    current_database = db_name;
    std::cout << ">>> Database changed to " << db_name << std::endl;
}

void Interpreter::EXEC_ALTER_TABLE() {
    API API;
    CatalogManager CM;
    int check_index;

    if (query.substr(6, 5) != "table")
        throw input_format_error();

    std::string table_name = getWord(12, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();

    check_index++;
    std::string operation = getWord(check_index, check_index);
    operation = getLower(operation, 0);

    if (operation == "add") {
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        check_index++;
        short type = getType(check_index, check_index);
        check_index++;

        bool unique = false;
        if (check_index + 1 < (int)query.length() && !isEnd(check_index + 1)) {
            std::string unique_check = getWord(check_index + 1, check_index);
            if (getLower(unique_check, 0) == "unique") {
                unique = true;
            }
        }

        API.alterTableAddColumn(table_name, attr_name, type, unique);
        std::cout << ">>> SUCCESS" << std::endl;
    }
    else if (operation == "drop") {
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        if (!isEnd(check_index + 1))
            throw input_format_error();

        API.alterTableDropColumn(table_name, attr_name);
        std::cout << ">>> SUCCESS" << std::endl;
    }
    else if (operation == "modify") {
        check_index++;
        std::string next_word = getWord(check_index, check_index);
        std::string lower_next = getLower(next_word, 0);

        std::string attr_name;
        if (lower_next == "column") {
            check_index++;
            attr_name = getWord(check_index, check_index);
        } else {
            attr_name = next_word;
        }

        check_index++;
        short new_type = getType(check_index, check_index);
        check_index++;

        bool unique = false;
        if (check_index + 1 < (int)query.length() && !isEnd(check_index + 1)) {
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
    if (query[check_index + 1] != '(')
        throw input_format_error();
    attr_name = getWord(check_index + 3, check_index);
    if (query[check_index + 1] != ')' || !isEnd(check_index + 3))
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
    index_name = getWord(11, check_index);
    check_index++;
    if (getLower(query, check_index).substr(check_index, 2) != "on")
        throw input_format_error();
    table_name = getWord(check_index + 3, check_index);
    if (!isEnd(check_index + 1))
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
    std::string file_path = getWord(9, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    std::fstream fs(file_path);
    if (!fs.is_open())
        throw input_format_error();
    std::stringstream ss;
    ss << fs.rdbuf();
    std::string file_content = ss.str();
    int start = 0;
    for (int i = 0; i < (int)file_content.length(); i++) {
        if (file_content[i] == ';') {
            std::string stmt = file_content.substr(start, i - start + 1);
            bool has_content = false;
            for (int j = 0; j < (int)stmt.length(); j++) {
                if (stmt[j] != ' ' && stmt[j] != '\n' && stmt[j] != '\r' && stmt[j] != '\t') {
                    has_content = true;
                    break;
                }
            }
            if (has_content) {
                query = stmt;
                for (int k = 0; k < (int)query.length(); k++) {
                    if (query[k] == '\n' || query[k] == '\r')
                        query[k] = ' ';
                }
                query = trimTrailing(query);
                if (!query.empty()) {
                    query += ' ';
                    Normalize();
                    EXEC();
                }
            }
            start = i + 1;
        }
    }
}

// ===== DESCRIBE/DESC <table_name> =====
// 显示表的结构信息（属性、类型、索引等）
void Interpreter::EXEC_SHOW() {
    CatalogManager CM;
    std::string table_name;
    int check_index;
    getWord(0, check_index);
    table_name = getWord(check_index + 1, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    CM.showTable(table_name);
}

void Interpreter::EXEC_SHOW_DATABASES() {
    int check_index;
    std::string second_word = getWord(5, check_index);
    std::string lower_second = getLower(second_word, 0);
    if (lower_second != "databases")
        throw input_format_error();
    if (!isEnd(check_index + 1))
        throw input_format_error();

    std::string db_path = "./database";
    _finddata_t file_info;
    intptr_t handle = _findfirst((db_path + "/*").c_str(), &file_info);
    if (handle == -1) {
        std::cout << ">>> No databases found." << std::endl;
        return;
    }
    std::cout << ">>> Databases:" << std::endl;
    int count = 0;
    do {
        if (strcmp(file_info.name, ".") != 0 && strcmp(file_info.name, "..") != 0) {
            if (file_info.attrib & _A_SUBDIR) {
                std::cout << "    " << file_info.name;
                if (current_database == file_info.name)
                    std::cout << " (current)";
                std::cout << std::endl;
                count++;
            }
        }
    } while (_findnext(handle, &file_info) == 0);
    _findclose(handle);
    if (count == 0)
        std::cout << "    (none)" << std::endl;
}

void Interpreter::EXEC_UPDATE() {
    API API;
    CatalogManager CM;
    std::string table_name;
    std::vector<std::string> attr_names;
    std::vector<Data> values;
    std::vector<std::string> target_name;
    std::vector<Where> where_select;
    Where tmp_where;
    std::string relation;
    char op = 0;
    int check_index;

    table_name = getWord(7, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();
    Attribute tmp_attr = CM.getAttribute(table_name);

    if (getLower(query, check_index + 1).substr(check_index + 1, 3) != "set")
        throw input_format_error();
    check_index += 5;

    while (1) {
        std::string attr_name = getWord(check_index, check_index);
        if (!CM.hasAttribute(table_name, attr_name))
            throw attribute_not_exist();
        attr_names.push_back(attr_name);

        check_index++;
        if (query[check_index] != '=')
            throw input_format_error();
        check_index += 2;

        std::string value_str = getWord(check_index, check_index);
        Data value_data;
        int attr_idx = -1;
        for (int i = 0; i < tmp_attr.num; i++) {
            if (tmp_attr.name[i] == attr_name) {
                attr_idx = i;
                break;
            }
        }
        value_data.type = tmp_attr.type[attr_idx];
        switch (value_data.type) {
        case -1:
            try { value_data.datai = stringToNum<int>(value_str); }
            catch (...) { throw data_type_conflict(); }
            break;
        case 0:
            try { value_data.dataf = stringToNum<float>(value_str); }
            catch (...) { throw data_type_conflict(); }
            break;
        default:
            try {
                if (!(value_str[0] == '\'' && value_str[value_str.length() - 1] == '\'') && !(value_str[0] == '"' && value_str[value_str.length() - 1] == '"'))
                    throw input_format_error();
                value_data.datas = value_str.substr(1, value_str.length() - 2);
            }
            catch (input_format_error error) { throw input_format_error(); }
            catch (...) { throw data_type_conflict(); }
            break;
        }
        values.push_back(value_data);

        check_index++;
        if (query[check_index] == ',') {
            check_index += 2;
            continue;
        }
        else {
            break;
        }
    }

    if (isEnd(check_index)) {
        std::vector<std::string> empty_target;
        std::vector<Where> empty_where;
        int count = API.updateRecord(table_name, attr_names, values, empty_target, empty_where, op);
        std::cout << ">>> " << count << " record(s) updated" << std::endl;
    }
    else {
        if (getLower(query, check_index).substr(check_index, 5) != "where")
            throw input_format_error();
        check_index += 6;

        while (1) {
            std::string tmp_target_name = getWord(check_index, check_index);
            if (!CM.hasAttribute(table_name, tmp_target_name))
                throw attribute_not_exist();
            target_name.push_back(tmp_target_name);

            relation = getRelation(check_index + 1, check_index);
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

            std::string tmp_value = getWord(check_index + 1, check_index);
            for (int i = 0; i < tmp_attr.num; i++) {
                if (tmp_target_name == tmp_attr.name[i]) {
                    tmp_where.data.type = tmp_attr.type[i];
                    switch (tmp_where.data.type) {
                    case -1:
                        try { tmp_where.data.datai = stringToNum<int>(tmp_value); }
                        catch (...) { throw data_type_conflict(); }
                        break;
                    case 0:
                        try { tmp_where.data.dataf = stringToNum<float>(tmp_value); }
                        catch (...) { throw data_type_conflict(); }
                        break;
                    default:
                        try {
                            if (!(tmp_value[0] == '\'' && tmp_value[tmp_value.length() - 1] == '\'') && !(tmp_value[0] == '"' && tmp_value[tmp_value.length() - 1] == '"'))
                                throw input_format_error();
                            tmp_where.data.datas = tmp_value.substr(1, tmp_value.length() - 2);
                        }
                        catch (input_format_error error) { throw input_format_error(); }
                        catch (...) { throw data_type_conflict(); }
                    }
                    break;
                }
            }

            where_select.push_back(tmp_where);

            if (isEnd(check_index + 1))
                break;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 3) == "and")
                op = 1;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 2) == "or")
                op = 0;
            else
                throw input_format_error();
            getWord(check_index + 1, check_index);
            check_index++;
        }

        int count = API.updateRecord(table_name, attr_names, values, target_name, where_select, op);
        std::cout << ">>> " << count << " record(s) updated" << std::endl;
    }
}

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
    table_name = getWord(12, check_index);
    if (!CM.hasTable(table_name))
        throw table_not_exist();

    if (isEnd(check_index + 1)) {
        attr_name = "";
        API.deleteRecord(table_name, attr_name, where_delete);
        std::cout << ">>> SUCCESS" << std::endl;
        return;
    }

    // 解析WHERE条件
    if (getLower(query, check_index + 1).substr(check_index + 1, 5) != "where")
        throw input_format_error();
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
    while (!isEnd(check_index + 1) && query[check_index + 1] != ')') {
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
    if (isEnd(check_index + 1))
        throw input_format_error();
    if (num_of_insert != attr_exist.num)
        throw input_format_error();
    API.insertRecord(table_name, tuple_insert);
    std::cout << ">>> SUCCESS" << std::endl;
}

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
    int aggregate_func = 0;
    std::string aggregate_attr;
    std::string first_word_raw = getWord(7, check_index);
    std::string first_word = getLower(first_word_raw, 0);
    size_t paren_pos = first_word.find('(');
    if (paren_pos != std::string::npos)
        first_word = first_word.substr(0, paren_pos);

    if (first_word == "count" || first_word == "sum" || first_word == "avg" || first_word == "min" || first_word == "max") {
        if (first_word == "count") aggregate_func = 1;
        else if (first_word == "sum") aggregate_func = 2;
        else if (first_word == "avg") aggregate_func = 3;
        else if (first_word == "min") aggregate_func = 4;
        else if (first_word == "max") aggregate_func = 5;
        int paren_start = -1;
        for (int i = 7; i < (int)query.length(); i++) {
            if (query[i] == '(') { paren_start = i; break; }
        }
        if (paren_start < 0) throw input_format_error();
        int paren_end = -1;
        for (int i = paren_start + 1; i < (int)query.length(); i++) {
            if (query[i] == ')') { paren_end = i; break; }
        }
        if (paren_end < 0) throw input_format_error();
        aggregate_attr = query.substr(paren_start + 1, paren_end - paren_start - 1);
        while (!aggregate_attr.empty() && aggregate_attr[0] == ' ') aggregate_attr = aggregate_attr.substr(1);
        while (!aggregate_attr.empty() && aggregate_attr[aggregate_attr.length()-1] == ' ') aggregate_attr = aggregate_attr.substr(0, aggregate_attr.length()-1);
        if (aggregate_attr != "*" && aggregate_func != 1) {
            std::string lower_attr = getLower(aggregate_attr, 0);
            aggregate_attr = lower_attr;
        }
        check_index = paren_end + 1;
        while (check_index < (int)query.length() && query[check_index] == ' ')
            check_index++;
        if (aggregate_attr == "*") flag = 1;
    }
    else if (getWord(7, check_index) == "*")
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
    if (!CM.hasTable(table_name)) {
        std::string view_path;
        if (current_database.empty()) {
            view_path = "./database/catalog/" + table_name + ".view";
        } else {
            view_path = "./database/" + current_database + "/catalog/" + table_name + ".view";
        }
        std::ifstream vfs(view_path);
        if (vfs.is_open()) {
            std::stringstream vss;
            vss << vfs.rdbuf();
            vfs.close();
            std::string view_select = vss.str();
            query = view_select;
            Normalize();
            EXEC_SELECT();
            return;
        }
        throw table_not_exist();
    }
    Attribute tmp_attr = CM.getAttribute(table_name);
    if (aggregate_func > 0 && aggregate_attr != "*") {
        bool found = false;
        for (int i = 0; i < tmp_attr.num; i++) {
            if (tmp_attr.name[i] == aggregate_attr) { found = true; break; }
        }
        if (!found) throw attribute_not_exist();
    }
    if (!flag && aggregate_func == 0) {
        for (int index = 0; index < (int)attr_name.size(); index++) {
            if (!CM.hasAttribute(table_name, attr_name[index]))
                throw attribute_not_exist();
        }
    }
    if (flag && aggregate_func == 0) {
        for (int index = 0; index < tmp_attr.num; index++) {
            attr_name.push_back(tmp_attr.name[index]);
        }
    }
    check_index++;
    if (isEnd(check_index))
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
            if (isEnd(check_index + 1))
                break;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 3) == "and")
                op = 1;
            else if (getLower(query, check_index + 1).substr(check_index + 1, 2) == "or")
                op = 0;
            else
                throw input_format_error();
            getWord(check_index + 1, check_index);
            check_index++;
        }

        output_table = API.selectRecord(table_name, target_name, where_select, op);
    }

    if (aggregate_func > 0) {
        std::vector<Tuple> tuples = output_table.getTuple();
        int count = 0;
        int agg_idx = -1;
        if (aggregate_attr != "*") {
            for (int i = 0; i < tmp_attr.num; i++) {
                if (tmp_attr.name[i] == aggregate_attr) { agg_idx = i; break; }
            }
        }
        float sum_val = 0;
        float min_val = 0, max_val = 0;
        bool first = true;
        for (int i = 0; i < (int)tuples.size(); i++) {
            if (tuples[i].isDeleted()) continue;
            count++;
            if (agg_idx >= 0 && agg_idx < (int)tuples[i].getData().size()) {
                Data d = tuples[i].getData()[agg_idx];
                float val = 0;
                if (d.type == -1) val = (float)d.datai;
                else if (d.type == 0) val = d.dataf;
                else val = 0;
                sum_val += val;
                if (first) { min_val = val; max_val = val; first = false; }
                else { if (val < min_val) min_val = val; if (val > max_val) max_val = val; }
            }
        }
        std::string func_name;
        switch (aggregate_func) {
        case 1: func_name = "COUNT"; std::cout << ">>> " << count << std::endl; return;
        case 2: func_name = "SUM"; std::cout << ">>> " << sum_val << std::endl; return;
        case 3: func_name = "AVG"; if (count > 0) std::cout << ">>> " << sum_val / count << std::endl; else std::cout << ">>> 0" << std::endl; return;
        case 4: func_name = "MIN"; std::cout << ">>> " << min_val << std::endl; return;
        case 5: func_name = "MAX"; std::cout << ">>> " << max_val << std::endl; return;
        }
        return;
    }

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
    for (int index = 0; index < (int)attr_name.size(); index++) {
        if (index != (int)attr_name.size() - 1) {
            for (int i = 0; i < (longest - (int)attr_record.name[use[index]].length()) / 2; i++)
                std::cout << " ";
            std::cout << attr_record.name[use[index]];
            for (int i = 0; i < longest - (longest - (int)attr_record.name[use[index]].length()) / 2 - (int)attr_record.name[use[index]].length(); i++)
                std::cout << " ";
            std::cout << "|";
        }
        else {
            for (int i = 0; i < (longest - (int)attr_record.name[use[index]].length()) / 2; i++)
                std::cout << " ";
            std::cout << attr_record.name[use[index]];
            for (int i = 0; i < longest - (longest - (int)attr_record.name[use[index]].length()) / 2 - (int)attr_record.name[use[index]].length(); i++)
                std::cout << " ";
            std::cout << "\n";
        }
    }
    for (int index = 0; index < (int)attr_name.size() * (longest + 1); index++) {
        std::cout << "-";
    }
    std::cout << std::endl;
    for (int index = 0; index < (int)output_tuple.size(); index++) {
        for (int i = 0; i < (int)attr_name.size(); i++)
        {
            switch (output_tuple[index].getData()[use[i]].type) {
            case -1:
                if (i != (int)attr_name.size() - 1) {
                    int len = output_tuple[index].getData()[use[i]].datai;
                    len = getBits(len);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        std::cout << " ";
                    std::cout << output_tuple[index].getData()[use[i]].datai;
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        std::cout << " ";
                    std::cout << "|";
                }
                else {
                    int len = output_tuple[index].getData()[use[i]].datai;
                    len = getBits(len);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        std::cout << " ";
                    std::cout << output_tuple[index].getData()[use[i]].datai;
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        std::cout << " ";
                    std::cout << "\n";
                }
                break;
            case 0:
                if (i != (int)attr_name.size() - 1) {
                    float num = output_tuple[index].getData()[use[i]].dataf;
                    int len = getBits(num);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        std::cout << " ";
                    std::cout << std::fixed << std::setprecision(2) << output_tuple[index].getData()[use[i]].dataf;
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        std::cout << " ";
                    std::cout << "|";
                }
                else {
                    float num = output_tuple[index].getData()[use[i]].dataf;
                    int len = getBits(num);
                    for (int i = 0; i < (longest - len) / 2; i++)
                        std::cout << " ";
                    std::cout << std::fixed << std::setprecision(2) << output_tuple[index].getData()[use[i]].dataf;
                    for (int i = 0; i < longest - (longest - len) / 2 - len; i++)
                        std::cout << " ";
                    std::cout << "\n";
                }
                break;
            default:
                std::string tmp = output_tuple[index].getData()[use[i]].datas;
                if (i != (int)attr_name.size() - 1) {
                    for (int i = 0; i < (longest - (int)tmp.length()) / 2; i++)
                        std::cout << " ";
                    std::cout << tmp;
                    for (int i = 0; i < longest - (longest - (int)tmp.length()) / 2 - (int)tmp.length(); i++)
                        std::cout << " ";
                    std::cout << "|";
                }
                else {
                    for (int i = 0; i < (longest - (int)tmp.length()) / 2; i++)
                        std::cout << " ";
                    std::cout << tmp;
                    for (int i = 0; i < longest - (longest - (int)tmp.length()) / 2 - (int)tmp.length(); i++)
                        std::cout << " ";
                    std::cout << "\n";
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
        if (isEnd(check_index)) {
            if (isEnd(check_index - 2))
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
        if (check_index + 1 < (int)query.length() && (query[check_index + 1] == 'u' || query[check_index + 1] == 'U')) {
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

void Interpreter::EXEC_DROP_TABLE() {
    API API;
    std::string table_name;
    int check_index;
    table_name = getWord(11, check_index);
    if (!isEnd(check_index + 1))
        throw input_format_error();
    API.dropTable(table_name);
    std::cout << ">>> SUCCESS" << std::endl;
}

short Interpreter::getType(int pos, int& end_pos) {
    std::string type = getWord(pos, end_pos);
    for (int i = 0; i < (int)type.length(); i++)
        if (type[i] >= 'A' && type[i] <= 'Z')
            type[i] += 32;

    if (type == "int")
        return -1;
    else if (type == "float")
        return 0;
    else if (type == "char" || type == "varchar") {
        end_pos += 3;
        std::string length = getWord(end_pos, end_pos);
        end_pos += 2;
        // 返回长度+1，因为0已被float占用，需要+1来区分
        return atoi(length.c_str()) + 1;
    }
    throw input_format_error();
}

std::string Interpreter::getWord(int pos, int& end_pos) {
    for (int pos1 = pos; pos1 < (int)query.length(); pos1++) {
        if (query[pos1] == ' ')
        {
            end_pos = pos1;
            return query.substr(pos, pos1 - pos);
        }
    }
    end_pos = (int)query.length() - 1;
    return query.substr(pos);
}

std::string Interpreter::getLower(std::string str, int pos) {
    for (int index = pos; index < (int)str.length(); index++) {
        if (str[index] == ' ')
            break;
        else if (str[index] >= 'A' && str[index] <= 'Z')
            str[index] += 32;
    }
    return str;
}

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
