// api.cpp - API层
// 作为Interpreter和底层管理器之间的桥梁，
// 提供统一的数据库操作接口，协调各管理器的工作。
//
// 主要职责：
// - 表的创建和删除（协调CatalogManager和RecordManager）
// - 索引的创建和删除（协调CatalogManager、RecordManager和IndexManager）
// - 记录的增删查改
// - 数据库的创建和删除
// - ALTER TABLE操作（字段添加、删除、修改）

#include "api.h"
#include <direct.h>
#include <io.h>
#include <sstream>

// 辅助函数：删除表的所有索引文件
// ALTER TABLE重建表时必须调用，否则旧索引数据会指向已失效的记录位置
static void deleteIndexFiles(std::string table_name) {
    CatalogManager catalog_manager;
    Attribute attr = catalog_manager.getAttribute(table_name);
    Index index_info = catalog_manager.getIndex(table_name);
    for (int i = 0; i < index_info.num; i++) {
        std::string attr_name = attr.name[index_info.location[i]];
        std::string file_path;
        if (current_database.empty()) {
            file_path = "./database/index/INDEX_FILE_" + attr_name + "_" + table_name;
        } else {
            file_path = "./database/" + current_database + "/index/INDEX_FILE_" + attr_name + "_" + table_name;
        }
        remove(file_path.c_str());
    }
}

// 辅助函数：重建表的所有索引
// ALTER TABLE重建表后调用，为所有有索引的属性重新建立B+树
void API::rebuildIndexes(std::string table_name) {
    CatalogManager catalog_manager;
    RecordManager record_manager;
    Attribute attr = catalog_manager.getAttribute(table_name);
    IndexManager index_manager(table_name);

    for (int i = 0; i < attr.num; i++) {
        if (attr.has_index[i]) {
            std::string attr_name = attr.name[i];
            record_manager.createIndex(index_manager, table_name, attr_name);
        }
    }
}

// 根据current_database返回索引文件的完整路径
// 当current_database为空时使用默认路径，否则使用数据库专属路径
std::string API::getIndexFilePath(std::string file_path) {
    if (current_database.empty()) {
        return "./database/index/" + file_path;
    }
    return "./database/" + current_database + "/index/" + file_path;
}

// 创建表：先在catalog中注册元信息，再创建数据文件
bool API::createTable(std::string table_name, Attribute attr, int primary, Index index) {
    CatalogManager catalog_manager;
    RecordManager record_manager;
    catalog_manager.createTable(table_name, attr, primary, index);
    record_manager.createTableFile(table_name);
    return true;
}

// 删除表：先删除catalog中的元信息，再删除数据文件
bool API::dropTable(std::string table_name) {
    CatalogManager catalog_manager;
    RecordManager record_manager;
    catalog_manager.dropTable(table_name);
    record_manager.dropTableFile(table_name);
    return true;
}

// 创建索引：先在catalog中注册索引，再对已有记录建立B+树索引
bool API::createIndex(std::string table_name, std::string attr_name, std::string index_name) {
    CatalogManager catalog_manager;
    RecordManager record_manager;
    IndexManager index_manager(table_name);
    catalog_manager.createIndex(table_name, attr_name, index_name);
    record_manager.createIndex(index_manager, table_name, attr_name);
    return true;
}

// 删除索引：先从B+树中删除，再从catalog中删除索引记录
bool API::dropIndex(std::string table_name, std::string index_name) {
    CatalogManager catalog_manager;
    IndexManager index_manager(table_name);
    // 通过索引名找到对应的属性名
    std::string attr_name = catalog_manager.IndextoAttr(table_name, index_name);
    // 获取属性类型，dropIndex需要type参数来确定索引所在的map
    Attribute attr = catalog_manager.getAttribute(table_name);
    int type = -1;
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == attr_name) {
            type = attr.type[i];
            break;
        }
    }
    // 使用与IndexManager构造函数中相同的路径格式作为map的key
    std::string file_path = "INDEX_FILE_" + attr_name + "_" + table_name;
    index_manager.dropIndex(file_path, type);
    catalog_manager.dropIndex(table_name, index_name);
    return true;
}

// 插入记录
bool API::insertRecord(std::string table_name, Tuple& tuple) {
    RecordManager record_manager;
    record_manager.insertRecord(table_name, tuple);
    return true;
}

// 删除表中所有记录
int API::deleteRecord(std::string table_name) {
    RecordManager record_manager;
    return record_manager.deleteRecord(table_name);
}

// 条件删除：删除满足WHERE条件的记录
int API::deleteRecord(std::string table_name, std::string target_attr, Where where) {
    RecordManager record_manager;
    return record_manager.deleteRecord(table_name, target_attr, where);
}

// 更新记录：修改满足WHERE条件的记录中指定字段的值
// 实现方式：查询满足条件的记录 → 修改字段值 → 删除旧记录 → 插入新记录
int API::updateRecord(std::string table_name, std::vector<std::string> attr_names, std::vector<Data> values, std::vector<std::string> target_name, std::vector<Where> where, char op) {
    RecordManager record_manager;
    CatalogManager catalog_manager;
    Attribute attr = catalog_manager.getAttribute(table_name);

    // 获取需要更新的记录
    Table old_table;
    if (where.size() == 0) {
        old_table = record_manager.selectRecord(table_name);
    } else {
        old_table = selectRecord(table_name, target_name, where, op);
    }
    std::vector<Tuple> old_tuples = old_table.getTuple();

    // 构建属性名到索引的映射
    int attr_index[32];
    for (int k = 0; k < (int)attr_names.size(); k++) {
        attr_index[k] = -1;
        for (int i = 0; i < attr.num; i++) {
            if (attr.name[i] == attr_names[k]) {
                attr_index[k] = i;
                break;
            }
        }
    }

    // 修改每条记录的指定字段
    std::vector<Tuple> new_tuples;
    int count = 0;
    for (int i = 0; i < (int)old_tuples.size(); i++) {
        if (old_tuples[i].isDeleted())
            continue;
        std::vector<Data> data = old_tuples[i].getData();
        // 更新指定字段的值
        for (int k = 0; k < (int)attr_names.size(); k++) {
            if (attr_index[k] >= 0 && attr_index[k] < (int)data.size()) {
                data[attr_index[k]] = values[k];
            }
        }
        // 构造新元组
        Tuple new_tuple;
        for (int j = 0; j < (int)data.size(); j++) {
            new_tuple.addData(data[j]);
        }
        new_tuples.push_back(new_tuple);
        count++;
    }

    // 删除满足条件的旧记录
    if (where.size() == 0) {
        record_manager.deleteRecord(table_name);
    } else {
        for (int i = 0; i < (int)target_name.size() && i < (int)where.size(); i++) {
            record_manager.deleteRecord(table_name, target_name[i], where[i]);
            break;
        }
    }

    // 插入修改后的新记录
    for (int i = 0; i < (int)new_tuples.size(); i++) {
        record_manager.insertRecord(table_name, new_tuples[i]);
    }

    return count;
}

// 查询整张表
Table API::selectRecord(std::string table_name) {
    RecordManager record_manager;
    return record_manager.selectRecord(table_name);
}

// 条件查询：返回满足WHERE条件的记录
Table API::selectRecord(std::string table_name, std::string target_attr, Where where) {
    RecordManager record_manager;
    return record_manager.selectRecord(table_name, target_attr, where);
}

// 多条件查询：支持AND/OR组合的WHERE条件
// 采用全表查询+内存过滤的方式，避免conditionSelectInBlock的缓冲区兼容问题
// op: 0=OR（取并集），1=AND（取交集）
Table API::selectRecord(std::string table_name, std::vector<std::string> target_name, std::vector<Where> where, char op) {
    RecordManager record_manager;
    CatalogManager catalog_manager;
    Attribute attr = catalog_manager.getAttribute(table_name);
    Table result_table(table_name, attr);

    // 没有WHERE条件时，返回全表
    if (where.size() == 0) {
        return record_manager.selectRecord(table_name);
    }

    // 先获取全表数据
    Table all_table = record_manager.selectRecord(table_name);
    std::vector<Tuple> all_tuples = all_table.getTuple();

    // 对每条记录检查所有WHERE条件
    std::vector<Tuple>& result_tuples = result_table.getTuple();
    for (int i = 0; i < (int)all_tuples.size(); i++) {
        if (all_tuples[i].isDeleted())
            continue;
        std::vector<Data> d = all_tuples[i].getData();

        bool match = false;
        if (op == 1) {
            // AND: 该记录必须满足所有WHERE条件
            match = true;
            for (int j = 0; j < (int)target_name.size() && j < (int)where.size(); j++) {
                if (!checkWhereCondition(d, attr, target_name[j], where[j])) {
                    match = false;
                    break;
                }
            }
        } else {
            // OR: 该记录只需满足任一WHERE条件
            match = false;
            for (int j = 0; j < (int)target_name.size() && j < (int)where.size(); j++) {
                if (checkWhereCondition(d, attr, target_name[j], where[j])) {
                    match = true;
                    break;
                }
            }
        }

        if (match) {
            result_tuples.push_back(all_tuples[i]);
        }
    }

    return result_table;
}

// 检查一条记录是否满足单个WHERE条件
// 在记录的数据中找到目标属性，然后用isSatisfied模板函数比较
bool API::checkWhereCondition(std::vector<Data>& data, Attribute& attr, std::string target_attr, Where& where) {
    // 找到目标属性在属性表中的位置
    int index = -1;
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == target_attr) {
            index = i;
            break;
        }
    }
    if (index == -1 || index >= (int)data.size())
        return false;

    // 根据属性类型比较
    switch (attr.type[index]) {
    case -1:
        return isSatisfied(data[index].datai, where.data.datai, where.relation_character);
    case 0:
        return isSatisfied(data[index].dataf, where.data.dataf, where.relation_character);
    default:
        return isSatisfied(data[index].datas, where.data.datas, where.relation_character);
    }
}

// 显示表结构信息
void API::showTable(std::string table_name) {
    CatalogManager catalog_manager;
    catalog_manager.showTable(table_name);
}

// ===== 数据库相关方法实现 =====

// 创建数据库：创建目录结构和空的catalog文件
// 目录结构：./database/<db_name>/{catalog, data, index}
// catalog文件初始内容为'#'，表示空目录
bool API::createDatabase(std::string db_name) {
    CatalogManager catalog_manager;
    if (catalog_manager.hasDatabase(db_name))
        throw database_exist();

    _mkdir("./database");
    std::string base_path = "./database/" + db_name;
    if (_mkdir(base_path.c_str()) != 0)
        throw database_exist();
    _mkdir((base_path + "/catalog").c_str());
    _mkdir((base_path + "/data").c_str());
    _mkdir((base_path + "/index").c_str());

    std::string catalog_path = base_path + "/catalog/catalog_file";
    FILE* f = fopen(catalog_path.c_str(), "w");
    if (f) {
        fputc('#', f);
        fclose(f);
    }

    return true;
}

// 删除数据库：递归删除数据库目录下的所有文件和子目录
// 如果删除的是当前正在使用的数据库，会自动重置current_database
bool API::dropDatabase(std::string db_name) {
    CatalogManager catalog_manager;
    if (!catalog_manager.hasDatabase(db_name))
        throw database_not_exist();

    // 如果删除的是当前使用的数据库，重置current_database
    if (current_database == db_name)
        current_database = "";

    std::string base_path = "./database/" + db_name;

    // 递归删除目录中的所有文件和子目录
    _finddata_t file_info;
    std::string search_path;
    intptr_t handle;

    // 删除data目录中的文件
    search_path = base_path + "/data/*";
    handle = _findfirst(search_path.c_str(), &file_info);
    if (handle != -1) {
        do {
            if (strcmp(file_info.name, ".") != 0 && strcmp(file_info.name, "..") != 0) {
                std::string file_path = base_path + "/data/" + file_info.name;
                remove(file_path.c_str());
            }
        } while (_findnext(handle, &file_info) == 0);
        _findclose(handle);
    }

    // 删除index目录中的文件
    search_path = base_path + "/index/*";
    handle = _findfirst(search_path.c_str(), &file_info);
    if (handle != -1) {
        do {
            if (strcmp(file_info.name, ".") != 0 && strcmp(file_info.name, "..") != 0) {
                std::string file_path = base_path + "/index/" + file_info.name;
                remove(file_path.c_str());
            }
        } while (_findnext(handle, &file_info) == 0);
        _findclose(handle);
    }

    // 删除catalog文件
    std::string catalog_path = base_path + "/catalog/catalog_file";
    remove(catalog_path.c_str());

    // 删除所有子目录（必须先删文件再删目录）
    _rmdir((base_path + "/data").c_str());
    _rmdir((base_path + "/index").c_str());
    _rmdir((base_path + "/catalog").c_str());
    _rmdir(base_path.c_str());

    return true;
}

// ===== ALTER TABLE 相关方法实现 =====
//
// 关键设计：必须先读取旧记录，再更新catalog，最后迁移记录
// 因为selectRecord依赖catalog中的schema来解析二进制记录，
// 如果先更新catalog再用新schema读旧数据，会导致解析错误。
//
// ALTER TABLE操作流程：
// 1. 用旧schema读取所有已有记录
// 2. 更新catalog（修改属性定义）
// 3. 删除旧表文件并重新创建空表
// 4. 用新schema重新插入所有记录

// 为表添加一个新字段
// 新增字段的默认值：int为0，float为0.0，string为空串
bool API::alterTableAddColumn(std::string table_name, std::string attr_name, short type, bool unique) {
    CatalogManager catalog_manager;
    RecordManager record_manager;

    // 步骤1：在catalog更新之前，用旧schema读取所有已有记录
    Table old_table = record_manager.selectRecord(table_name);
    std::vector<Tuple> old_tuples = old_table.getTuple();

    // 步骤2：更新catalog（添加新属性）
    catalog_manager.addAttribute(table_name, attr_name, type, unique);

    // 步骤3：为每条记录添加新列的默认值
    std::vector<Tuple> new_tuples;
    for (int i = 0; i < (int)old_tuples.size(); i++) {
        if (old_tuples[i].isDeleted())
            continue;
        Tuple new_tuple;
        // 复制旧字段
        std::vector<Data> old_data = old_tuples[i].getData();
        for (int j = 0; j < (int)old_data.size(); j++) {
            new_tuple.addData(old_data[j]);
        }
        // 添加新列的默认值：int为0，float为0.0，string为空串
        Data default_data;
        default_data.type = type;
        switch (type) {
            case -1: default_data.datai = 0; break;
            case 0: default_data.dataf = 0.0f; break;
            default: default_data.datas = ""; break;
        }
        new_tuple.addData(default_data);
        new_tuples.push_back(new_tuple);
    }

    // 步骤4：删除旧索引文件、旧表文件并重新创建
    deleteIndexFiles(table_name);
    record_manager.dropTableFile(table_name);
    record_manager.createTableFile(table_name);

    // 步骤5：用新schema重新插入所有记录
    for (int i = 0; i < (int)new_tuples.size(); i++) {
        record_manager.insertRecord(table_name, new_tuples[i]);
    }

    // 步骤6：重建索引（为所有有索引的属性重新建立B+树）
    rebuildIndexes(table_name);

    return true;
}

// 删除表中的一个字段
// 需要先找到要删除的属性在旧schema中的位置，以便从记录中移除对应数据
bool API::alterTableDropColumn(std::string table_name, std::string attr_name) {
    CatalogManager catalog_manager;
    RecordManager record_manager;

    // 步骤1：在catalog更新之前，读取旧属性信息找到要删除的属性位置
    Attribute old_attr = catalog_manager.getAttribute(table_name);
    int drop_index = -1;
    for (int i = 0; i < old_attr.num; i++) {
        if (old_attr.name[i] == attr_name) {
            drop_index = i;
            break;
        }
    }

    // 步骤2：用旧schema读取所有已有记录
    Table old_table = record_manager.selectRecord(table_name);
    std::vector<Tuple> old_tuples = old_table.getTuple();

    // 步骤3：更新catalog（删除属性）
    catalog_manager.dropAttribute(table_name, attr_name);

    // 步骤4：从每条记录中移除指定列的数据
    std::vector<Tuple> new_tuples;
    for (int i = 0; i < (int)old_tuples.size(); i++) {
        if (old_tuples[i].isDeleted())
            continue;
        Tuple new_tuple;
        std::vector<Data> old_data = old_tuples[i].getData();
        // 跳过被删除列的数据
        for (int j = 0; j < (int)old_data.size(); j++) {
            if (j != drop_index) {
                new_tuple.addData(old_data[j]);
            }
        }
        new_tuples.push_back(new_tuple);
    }

    // 步骤5：删除旧索引文件、旧表文件并重新创建
    deleteIndexFiles(table_name);
    record_manager.dropTableFile(table_name);
    record_manager.createTableFile(table_name);

    // 步骤6：用新schema重新插入所有记录
    for (int i = 0; i < (int)new_tuples.size(); i++) {
        record_manager.insertRecord(table_name, new_tuples[i]);
    }

    // 步骤7：重建索引（为所有有索引的属性重新建立B+树）
    rebuildIndexes(table_name);

    return true;
}

// 修改表中一个字段的类型和约束
// 需要对已有记录中该字段的数据进行类型转换
// 支持的转换：int↔float↔string
bool API::alterTableModifyColumn(std::string table_name, std::string attr_name, short new_type, bool new_unique) {
    CatalogManager catalog_manager;
    RecordManager record_manager;

    // 步骤1：在catalog更新之前，读取旧属性信息找到要修改的属性位置
    Attribute old_attr = catalog_manager.getAttribute(table_name);
    int modify_index = -1;
    for (int i = 0; i < old_attr.num; i++) {
        if (old_attr.name[i] == attr_name) {
            modify_index = i;
            break;
        }
    }

    // 步骤2：用旧schema读取所有已有记录
    Table old_table = record_manager.selectRecord(table_name);
    std::vector<Tuple> old_tuples = old_table.getTuple();

    // 步骤3：更新catalog（修改属性）
    catalog_manager.modifyAttribute(table_name, attr_name, new_type, new_unique);

    // 步骤4：修改每条记录中指定列的数据类型
    std::vector<Tuple> new_tuples;
    for (int i = 0; i < (int)old_tuples.size(); i++) {
        if (old_tuples[i].isDeleted())
            continue;
        Tuple new_tuple;
        std::vector<Data> old_data = old_tuples[i].getData();
        for (int j = 0; j < (int)old_data.size(); j++) {
            if (j == modify_index) {
                // 对目标字段进行类型转换
                Data converted;
                converted.type = new_type;
                switch (new_type) {
                    case -1:
                        // 转换为int：float截断小数，string用atoi
                        switch (old_data[j].type) {
                            case 0: converted.datai = (int)old_data[j].dataf; break;
                            default: converted.datai = atoi(old_data[j].datas.c_str()); break;
                        }
                        break;
                    case 0:
                        // 转换为float：int直接转，string用atof
                        switch (old_data[j].type) {
                            case -1: converted.dataf = (float)old_data[j].datai; break;
                            default: converted.dataf = (float)atof(old_data[j].datas.c_str()); break;
                        }
                        break;
                    default:
                        // 转换为string：int/float用stringstream转，string直接复制
                        converted.datas = "";
                        switch (old_data[j].type) {
                            case -1: {
                                std::stringstream ss;
                                ss << old_data[j].datai;
                                converted.datas = ss.str();
                            }; break;
                            case 0: {
                                std::stringstream ss;
                                ss << old_data[j].dataf;
                                converted.datas = ss.str();
                            }; break;
                            default: converted.datas = old_data[j].datas; break;
                        }
                        break;
                }
                new_tuple.addData(converted);
            } else {
                // 非目标字段直接复制
                new_tuple.addData(old_data[j]);
            }
        }
        new_tuples.push_back(new_tuple);
    }

    // 步骤5：删除旧索引文件、旧表文件并重新创建
    deleteIndexFiles(table_name);
    record_manager.dropTableFile(table_name);
    record_manager.createTableFile(table_name);

    // 步骤6：用新schema重新插入所有记录
    for (int i = 0; i < (int)new_tuples.size(); i++) {
        record_manager.insertRecord(table_name, new_tuples[i]);
    }

    // 步骤7：重建索引（为所有有索引的属性重新建立B+树）
    rebuildIndexes(table_name);

    return true;
}
