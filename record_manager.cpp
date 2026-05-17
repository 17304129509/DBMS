// record_manager.cpp - 记录管理器
// 负责表数据文件的创建/删除，以及记录的增删查改。
// 记录以文本格式存储在页面缓冲区中，每条记录格式：
//   <长度4字节> <字段1> <字段2> ... <删除标记1字节> <换行符>
// 删除标记：'0'=有效记录，'1'=已删除记录

#include "record_manager.h"

// 根据current_database返回表数据文件的完整路径
// 当current_database为空时使用默认路径，否则使用数据库专属路径
std::string RecordManager::getDataFilePath(std::string table_name) {
    if (current_database.empty()) {
        return "./database/data/" + table_name;
    }
    return "./database/" + current_database + "/data/" + table_name;
}

// 创建表数据文件（空文件）
void RecordManager::createTableFile(std::string table_name) {
    std::string file_path = getDataFilePath(table_name);
    FILE* f = fopen(file_path.c_str(), "w");
    fclose(f);
}

// 删除表数据文件
// 必须先使缓冲区中该文件的缓存页失效，否则后续操作可能读到旧数据
void RecordManager::dropTableFile(std::string table_name) {
    std::string file_path = getDataFilePath(table_name);
    buffer_manager.invalidateFile(file_path);
    remove(file_path.c_str());
}

// 插入一条记录
// 1. 类型检查：验证插入数据与表定义的属性类型一致
// 2. 冲突检查：验证主键和unique属性不冲突
// 3. 写入页面：找到有足够空间的页面写入记录
// 4. 更新索引：对有索引的属性更新B+树
void RecordManager::insertRecord(std::string table_name, Tuple& tuple) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    // 类型检查
    std::vector<Data> v = tuple.getData();
    for (int i = 0; i < (int)v.size(); i++) {
        if (v[i].type != attr.type[i])
            throw tuple_type_conflict();
    }
    // 主键冲突检查
    Table table = selectRecord(tmp_name);
    std::vector<Tuple>& tuples = table.getTuple();
    if (attr.primary_key >= 0) {
        if (isConflict(tuples, v, attr.primary_key) == true)
            throw primary_key_conflict();
    }
    // unique属性冲突检查
    for (int i = 0; i < attr.num; i++) {
        if (attr.unique[i] == true) {
            if (isConflict(tuples, v, i) == true)
                throw unique_conflict();
        }
    }

    // 找到有足够空间的页面写入记录
    int block_num = getBlockNum(table_path);
    if (block_num <= 0)
        block_num = 1;
    char* p = buffer_manager.getPage(table_path, block_num - 1);
    int i;
    for (i = 0; p[i] != '\0' && i < PAGESIZE; i++);
    // 计算记录的总长度
    int j;
    int len = 0;
    for (j = 0; j < (int)v.size(); j++) {
        Data d = v[j];
        switch (d.type) {
        case -1: {
            int t = getDataLength(d.datai);
            len += t;
        }; break;
        case 0: {
            float t = getDataLength(d.dataf);
            len += t;
        }; break;
        default: {
            len += (int)d.datas.length();
        };
        }
    }
    // 加上长度字段(4) + 每个字段前的空格(num) + 删除标记(2) + 换行(1)
    len += (int)v.size() + 7;
    int block_offset;
    if (PAGESIZE - i >= len) {
        // 当前页有足够空间
        block_offset = block_num - 1;
        insertRecord1(p, i, len, v);
        int page_id = buffer_manager.getPageId(table_path, block_num - 1);
        buffer_manager.modifyPage(page_id);
    }
    else {
        // 当前页空间不足，使用新页面
        block_offset = block_num;
        char* p = buffer_manager.getPage(table_path, block_num);
        insertRecord1(p, 0, len, v);
        int page_id = buffer_manager.getPageId(table_path, block_num);
        buffer_manager.modifyPage(page_id);
    }

    // 更新索引：对有索引的属性在B+树中插入新键值
    IndexManager index_manager(tmp_name);
    for (int i = 0; i < attr.num; i++) {
        if (attr.has_index[i] == true) {
            std::string attr_name = attr.name[i];
            std::string file_path = "INDEX_FILE_" + attr_name + "_" + tmp_name;
            std::vector<Data> d = tuple.getData();
            index_manager.insertIndex(file_path, d[i], block_offset);
        }
    }
}

// 删除表中所有记录
// 遍历所有页面，将每条记录标记为已删除，并删除对应的索引
int RecordManager::deleteRecord(std::string table_name) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    int block_num = getBlockNum(table_path);
    if (block_num <= 0)
        return 0;
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    IndexManager index_manager(tmp_name);
    int count = 0;
    for (int i = 0; i < block_num; i++) {
        char* p = buffer_manager.getPage(table_path, i);
        char* t = p;
        while (*p != '\0' && p < t + PAGESIZE) {
            Tuple tuple = readTuple(p, attr);
            // 删除该记录的所有索引
            for (int j = 0; j < attr.num; j++) {
                if (attr.has_index[j] == true) {
                    std::string attr_name = attr.name[j];
                    std::string file_path = "INDEX_FILE_" + attr_name + "_" + tmp_name;
                    std::vector<Data> d = tuple.getData();
                    index_manager.deleteIndexByKey(file_path, d[j]);
                }
            }
            p = deleteRecord1(p);
            count++;
        }
        int page_id = buffer_manager.getPageId(table_path, i);
        buffer_manager.modifyPage(page_id);
    }
    return count;
}

// 条件删除：删除满足WHERE条件的记录
// 如果目标属性有索引且不是!=操作，则通过索引加速查找
int RecordManager::deleteRecord(std::string table_name, std::string target_attr, Where where) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    int index = -1;
    bool flag = false;
    // 找到目标属性的位置，并检查是否有索引
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == target_attr) {
            index = i;
            if (attr.has_index[i] == true)
                flag = true;
            break;
        }
    }
    if (index == -1) {
        throw attribute_not_exist();
    }
    else if (attr.type[index] != where.data.type) {
        throw data_type_conflict();
    }

    int count = 0;
    if (flag == true && where.relation_character != NOT_EQUAL) {
        // 有索引且非!=操作：通过索引缩小搜索范围
        std::vector<int> block_ids;
        searchWithIndex(tmp_name, target_attr, where, block_ids);
        for (int i = 0; i < (int)block_ids.size(); i++) {
            count += conditionDeleteInBlock(tmp_name, block_ids[i], attr, index, where);
        }
    }
    else {
        // 无索引或!=操作：全表扫描
        int block_num = getBlockNum(table_path);
        if (block_num <= 0)
            return 0;
        for (int i = 0; i < block_num; i++) {
            count += conditionDeleteInBlock(tmp_name, i, attr, index, where);
        }
    }
    return count;
}

// 查询整张表的所有记录
Table RecordManager::selectRecord(std::string table_name, std::string result_table_name) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    int block_num = getBlockNum(table_path);
    if (block_num <= 0)
        block_num = 1;
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    Table table(result_table_name, attr);
    std::vector<Tuple>& v = table.getTuple();
    // 遍历所有页面，读取未删除的记录
    for (int i = 0; i < block_num; i++) {
        char* p = buffer_manager.getPage(table_path, i);
        char* t = p;
        while (*p != '\0' && p < t + PAGESIZE) {
            Tuple tuple = readTuple(p, attr);
            if (tuple.isDeleted() == false)
                v.push_back(tuple);
            int len = getTupleLength(p);
            p = p + len;
        }
    }
    return table;
}

// 条件查询：返回满足WHERE条件的记录
// 如果目标属性有索引且不是!=操作，则通过索引加速查找
Table RecordManager::selectRecord(std::string table_name, std::string target_attr, Where where, std::string result_table_name) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    int index = -1;
    bool flag = false;
    // 找到目标属性的位置，并检查是否有索引
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == target_attr) {
            index = i;
            if (attr.has_index[i] == true)
                flag = true;
            break;
        }
    }
    if (index == -1) {
        throw attribute_not_exist();
    }
    else if (attr.type[index] != where.data.type) {
        throw data_type_conflict();
    }

    Table table(result_table_name, attr);
    std::vector<Tuple>& v = table.getTuple();
    if (flag == true && where.relation_character != NOT_EQUAL) {
        // 有索引且非!=操作：通过索引缩小搜索范围
        std::vector<int> block_ids;
        searchWithIndex(tmp_name, target_attr, where, block_ids);
        for (int i = 0; i < (int)block_ids.size(); i++) {
            conditionSelectInBlock(tmp_name, block_ids[i], attr, index, where, v);
        }
    }
    else {
        // 无索引或!=操作：全表扫描
        int block_num = getBlockNum(table_path);
        if (block_num <= 0)
            block_num = 1;
        for (int i = 0; i < block_num; i++) {
            conditionSelectInBlock(tmp_name, i, attr, index, where, v);
        }
    }
    return table;
}

// 为已有记录创建B+树索引
// 遍历所有记录，将指定属性的值和所在块号插入B+树
void RecordManager::createIndex(IndexManager& index_manager, std::string table_name, std::string target_attr) {
    std::string tmp_name = table_name;
    std::string table_path = getDataFilePath(table_name);
    CatalogManager catalog_manager;
    if (!catalog_manager.hasTable(tmp_name)) {
        throw table_not_exist();
    }
    Attribute attr = catalog_manager.getAttribute(tmp_name);
    int index = -1;
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == target_attr) {
            index = i;
            break;
        }
    }
    if (index == -1) {
        throw attribute_not_exist();
    }

    int block_num = getBlockNum(table_path);
    if (block_num <= 0)
        block_num = 1;
    std::string file_path = "INDEX_FILE_" + target_attr + "_" + tmp_name;
    for (int i = 0; i < block_num; i++) {
        char* p = buffer_manager.getPage(table_path, i);
        char* t = p;
        while (*p != '\0' && p < t + PAGESIZE) {
            Tuple tuple = readTuple(p, attr);
            if (tuple.isDeleted() == false) {
                std::vector<Data> v = tuple.getData();
                index_manager.insertIndex(file_path, v[index], i);
            }
            int len = getTupleLength(p);
            p = p + len;
        }
    }
}

// 获取文件的总块数
int RecordManager::getBlockNum(std::string table_name) {
    char* p;
    int block_num = -1;
    do {
        p = buffer_manager.getPage(table_name , block_num + 1);
        block_num++;
    } while(p[0] != '\0');
    return block_num;
}

// 将一条记录序列化写入页面缓冲区
// 格式：<长度4字节><空格><字段1><空格><字段2>...<空格><删除标记><换行>
void RecordManager::insertRecord1(char* p, int offset, int len, const std::vector<Data>& v) {
    std::stringstream stream;
    stream << len;
    std::string s = stream.str();
    // 长度字段固定4位，不足补0
    while (s.length() < 4)
        s = "0" + s;
    for (int j = 0; j < (int)s.length(); j++, offset++)
        p[offset] = s[j];
    // 逐个字段写入
    for (int j = 0; j < (int)v.size(); j++) {
        p[offset] = ' ';
        offset++;
        Data d = v[j];
        switch (d.type) {
        case -1: {
            copyString(p, offset, d.datai);
        }; break;
        case 0: {
            copyString(p, offset, d.dataf);
        }; break;
        default: {
            copyString(p, offset, d.datas);
        };
        }
    }
    // 删除标记'0'表示有效记录，换行符表示记录结束
    p[offset] = ' ';
    p[offset + 1] = '0';
    p[offset + 2] = '\n';
}

// 逻辑删除一条记录：将删除标记从'0'改为'1'
char* RecordManager::deleteRecord1(char* p) {
    int len = getTupleLength(p);
    p = p + len;
    *(p - 2) = '1';
    return p;
}

// 从页面缓冲区中读取一条记录并反序列化为Tuple对象
Tuple RecordManager::readTuple(const char* p, Attribute attr) {
    Tuple tuple;
    p = p + 5; // 跳过长度字段(4字节)和空格(1字节)
    for (int i = 0; i < attr.num; i++) {
        Data data;
        data.type = attr.type[i];
        char tmp[100];
        int j;
        // 读取到空格为止，得到一个字段的字符串表示
        for (j = 0; *p != ' '; j++, p++) {
            tmp[j] = *p;
        }
        tmp[j] = '\0';
        p++;
        std::string s(tmp);
        // 根据属性类型将字符串转为对应的数据类型
        switch (data.type) {
        case -1: {
            std::stringstream stream(s);
            stream >> data.datai;
        }; break;
        case 0: {
            std::stringstream stream(s);
            stream >> data.dataf;
        }; break;
        default: {
            data.datas = s;
        }
        }
        tuple.addData(data);
    }
    // 检查删除标记：'1'表示已删除
    if (*p == '1')
        tuple.setDeleted();
    return tuple;
}

// 获取记录的长度（从记录头部的4字节长度字段解析）
int RecordManager::getTupleLength(char* p) {
    char tmp[10];
    int i;
    for (i = 0; p[i] != ' '; i++)
        tmp[i] = p[i];
    tmp[i] = '\0';
    std::string s(tmp);
    int len = stoi(s);
    return len;
}

// 检查新记录与已有记录是否冲突（主键或unique属性）
bool RecordManager::isConflict(std::vector<Tuple>& tuples, std::vector<Data>& v, int index) {
    for (int i = 0; i < (int)tuples.size(); i++) {
        if (tuples[i].isDeleted() == true)
            continue;
        std::vector<Data> d = tuples[i].getData();
        switch (v[index].type) {
        case -1: {
            if (v[index].datai == d[index].datai)
                return true;
        }; break;
        case 0: {
            if (v[index].dataf == d[index].dataf)
                return true;
        }; break;
        default: {
            if (v[index].datas == d[index].datas)
                return true;
        };
        }
    }
    return false;
}

// 通过B+树索引搜索满足条件的记录所在块号
// 根据不同的关系运算符构造搜索范围
void RecordManager::searchWithIndex(std::string table_name, std::string target_attr, Where where, std::vector<int>& block_ids) {
    IndexManager index_manager(table_name);
    Data tmp_data;
    std::string file_path = "INDEX_FILE_" + target_attr + "_" + table_name;
    if (where.relation_character == LESS || where.relation_character == LESS_OR_EQUAL) {
        // <或<=：搜索范围从最小值到where值
        if (where.data.type == -1) {
            tmp_data.type = -1;
            tmp_data.datai = -INF;
        }
        else if (where.data.type == 0) {
            tmp_data.type = 0;
            tmp_data.dataf = -INF;
        }
        else {
            tmp_data.type = 1;
            tmp_data.datas = "";
        }
        index_manager.searchRange(file_path, tmp_data, where.data, block_ids);
    }
    else if (where.relation_character == GREATER || where.relation_character == GREATER_OR_EQUAL) {
        // >或>=：搜索范围从where值到最大值
        if (where.data.type == -1) {
            tmp_data.type = -1;
            tmp_data.datai = INF;
        }
        else if (where.data.type == 0) {
            tmp_data.type = 0;
            tmp_data.dataf = INF;
        }
        else {
            tmp_data.type = -2;
        }
        index_manager.searchRange(file_path, where.data, tmp_data, block_ids);
    }
    else {
        // =：搜索范围从where值到where值
        index_manager.searchRange(file_path, where.data, where.data, block_ids);
    }
}

// 在指定块中删除满足WHERE条件的记录
int RecordManager::conditionDeleteInBlock(std::string table_name, int block_id, Attribute attr, int index, Where where) {
    std::string table_path = getDataFilePath(table_name);
    char* p = buffer_manager.getPage(table_path, block_id);
    char* t = p;
    int count = 0;
    while (*p != '\0' && p < t + PAGESIZE) {
        Tuple tuple = readTuple(p, attr);
        std::vector<Data> d = tuple.getData();
        // 根据属性类型判断条件是否满足
        switch (attr.type[index]) {
        case -1: {
            if (isSatisfied(d[index].datai, where.data.datai, where.relation_character) == true) {
                p = deleteRecord1(p);
                count++;
            }
            else {
                int len = getTupleLength(p);
                p = p + len;
            }
        }; break;
        case 0: {
            if (isSatisfied(d[index].dataf, where.data.dataf, where.relation_character) == true) {
                p = deleteRecord1(p);
                count++;
            }
            else {
                int len = getTupleLength(p);
                p = p + len;
            }
        }; break;
        default: {
            if (isSatisfied(d[index].datas, where.data.datas, where.relation_character) == true) {
                p = deleteRecord1(p);
                count++;
            }
            else {
                int len = getTupleLength(p);
                p = p + len;
            }
        }
        }
    }
    int page_id = buffer_manager.getPageId(table_path, block_id);
    buffer_manager.modifyPage(page_id);
    return count;
}

// 在指定块中查询满足WHERE条件的记录
void RecordManager::conditionSelectInBlock(std::string table_name, int block_id, Attribute attr, int index, Where where, std::vector<Tuple>& v) {
    std::string table_path = getDataFilePath(table_name);
    char* p = buffer_manager.getPage(table_path, block_id);
    char* t = p;
    while (*p != '\0' && p < t + PAGESIZE) {
        Tuple tuple = readTuple(p, attr);
        if (tuple.isDeleted() == true) {
            int len = getTupleLength(p);
            p = p + len;
            continue;
        }
        std::vector<Data> d = tuple.getData();
        // 根据属性类型判断条件是否满足
        switch (attr.type[index]) {
        case -1: {
            if (isSatisfied(d[index].datai, where.data.datai, where.relation_character) == true) {
                v.push_back(tuple);
            }
        }; break;
        case 0: {
            if (isSatisfied(d[index].dataf, where.data.dataf, where.relation_character) == true) {
                v.push_back(tuple);
            }
        }; break;
        default: {
            if (isSatisfied(d[index].datas, where.data.datas, where.relation_character) == true) {
                v.push_back(tuple);
            }
        };
        }
        int len = getTupleLength(p);
        p = p + len;
    }
}
