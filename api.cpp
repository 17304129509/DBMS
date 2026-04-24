#include "api.h"
#include "template_function.h"
#include <algorithm>
#include <vector>
#include <iterator>

// 初始化类对象
API::API(){}

// 释放资源
API::~API(){}

// 按条件查询数据表记录，支持多条件逻辑组合筛选
Table API::selectRecord(std::string table_name, std::vector<std::string> target_attr, std::vector<Where> where, char operation)
{
	// 未指定查询字段，直接返回整张数据表
	if (target_attr.size() == 0) {
		return record.selectRecord(table_name);
	}
	// 单个查询条件，执行简单条件查询
	else if (target_attr.size() == 1) {
        return record.selectRecord(table_name, target_attr[0], where[0]);
    }
	// 多个查询条件，拆分两次查询后合并结果
	else {
		Table table1 = record.selectRecord(table_name, target_attr[0], where[0]);
		Table table2 = record.selectRecord(table_name, target_attr[1], where[1]);

		// 根据逻辑操作符选择结果合并方式
		if (operation)
			return joinTable(table1, table2, target_attr[0], where[0]);
		else
			return unionTable(table1, table2, target_attr[0], where[0]);
	}
}

// 删除数据表中符合条件的元组数据
int API::deleteRecord(std::string table_name , std::string target_attr , Where where)
{
    int result;
	// 无限定条件，清空整张表数据
	if (target_attr == "")
		result = record.deleteRecord(table_name);
	// 根据字段条件删除指定记录
	else
		result = record.deleteRecord(table_name, target_attr, where);
	return result;
}

// 向指定数据表中插入一条新的数据元组
void API::insertRecord(std::string table_name , Tuple& tuple)
{
	record.insertRecord(table_name, tuple);
	return;
}

// 新建数据表并保存表结构相关信息
bool API::createTable(std::string table_name, Attribute attribute, int primary, Index index)
{
	record.createTableFile(table_name);
	catalog.createTable(table_name, attribute, primary, index);

	return true;
}

// 删除指定数据表，同时清理对应数据文件与目录信息
bool API::dropTable(std::string table_name)
{
	record.dropTableFile(table_name);
	catalog.dropTable(table_name);

	return true;
}

// 为数据表指定字段建立索引结构，提升查询效率
bool API::createIndex(std::string table_name, std::string index_name, std::string attr_name)
{
    IndexManager index(table_name);
    
	std::string file_path = "INDEX_FILE_" + attr_name + "_" + table_name;
	int type;

	// 更新系统目录内的索引配置
	catalog.createIndex(table_name, attr_name, index_name);
	Attribute attr = catalog.getAttribute(table_name);
	for (int i = 0; i < attr.num; i++) {
		if (attr.name[i] == attr_name) {
			type = (int)attr.type[i];
			break;
		}
	}

	// 创建索引文件并写入对应索引数据
	index.createIndex(file_path, type);
	record.createIndex(index , table_name, attr_name);

	return true;
}

// 移除数据表中已创建的指定索引
bool API::dropIndex(std::string table_name, std::string index_name)
{
    IndexManager index(table_name);
    
	std::string attr_name = catalog.IndextoAttr(table_name, index_name);
	std::string file_path = "INDEX_FILE_" + attr_name + "_" + table_name;
	int type;

	Attribute attr = catalog.getAttribute(table_name);
	for (int i = 0; i < attr.num; i++) {
		if (attr.name[i] == attr_name) {
			type = (int)attr.type[i];
			break;
		}
	}

	// 删除索引文件并同步目录信息
	index.dropIndex(file_path, type);
	catalog.dropIndex(table_name, index_name);
	
	file_path = "./database/index/" + file_path;
    remove(file_path.c_str());
	return true;
}

// 合并两组查询结果，实现多条件或逻辑筛选
Table API::unionTable(Table &table1, Table &table2, std::string target_attr, Where where)
{
	Table result_table(table1);
    std::vector<Tuple>& result_tuple = result_table.getTuple();
	std::vector<Tuple> tuple1 = table1.getTuple();
	std::vector<Tuple> tuple2 = table2.getTuple();
    result_tuple = tuple1;

	// 遍历属性列表，定位目标条件字段下标
    int i;
    Attribute attr = table1.getAttr();
    for (i = 0; i < 32; i++)
        if (attr.name[i] == target_attr)
            break;
    
	// 筛选符合或逻辑的数据并入结果集
    for (int j = 0; j < tuple2.size(); j++)
        if (!isSatisfied(tuple2[j], i, where))
            result_tuple.push_back(tuple2[j]);
    
	// 对最终结果集进行排序整理
    std::sort(result_tuple.begin(), result_tuple.end(), sortcmp);
    return result_table;
}

// 取两组查询结果交集，实现多条件与逻辑筛选
Table API::joinTable(Table &table1, Table &table2, std::string target_attr, Where where)
{
	Table result_table(table1);
    std::vector<Tuple>& result_tuple = result_table.getTuple();
	std::vector<Tuple> tuple1 = table1.getTuple();
	std::vector<Tuple> tuple2 = table2.getTuple();
    
	// 查找需要判断的目标属性位置
    int i;
    Attribute attr = table1.getAttr();
    for (i = 0; i < 32; i++)
        if (attr.name[i] == target_attr)
            break;
    
	// 筛选满足共同条件的数据记录
    for (int j = 0; j < tuple2.size(); j++)
        if (isSatisfied(tuple2[j], i, where))
            result_tuple.push_back(tuple2[j]);
    
    std::sort(result_tuple.begin(), result_tuple.end(), sortcmp);
    return result_table;
}

// 自定义排序规则，根据首字段数据大小排序
bool sortcmp(const Tuple &tuple1, const Tuple &tuple2)
{
	std::vector<Data> data1 = tuple1.getData();
	std::vector<Data> data2 = tuple2.getData();

    switch (data1[0].type) {
        case -1: return data1[0].datai < data2[0].datai;
        case 0: return data1[0].dataf < data2[0].dataf;
        default: return data1[0].datas < data2[0].datas;
    }
}

// 对比两条元组内容，判断数据是否完全一致
bool calcmp(const Tuple &tuple1, const Tuple &tuple2)
{
	int i;

	std::vector<Data> data1 = tuple1.getData();
	std::vector<Data> data2 = tuple2.getData();

    for (i = 0; i < data1.size(); i++) {
        bool flag = false;
        switch (data1[0].type) {
            case -1: {
                if (data1[0].datai != data2[0].datai)
                    flag = true;
            }break;
            case 0: {
                if (data1[0].dataf != data2[0].dataf)
                    flag = true;
            }break;
            default: {
                if (data1[0].datas != data2[0].datas)
                    flag = true;
            }break;
        }
		if (flag)
			break;
    }

	if (i == data1.size())
		return true;
	else
		return false;
}

// 校验单条元组是否匹配设定的查询条件
bool isSatisfied(Tuple& tuple, int target_attr, Where where)
{
    std::vector<Data> data = tuple.getData();
    
    switch (where.relation_character) {
        case LESS : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai < where.data.datai); break;
                case 0 : return (data[target_attr].dataf < where.data.dataf); break;
                default: return (data[target_attr].datas < where.data.datas); break;
            }
        } break;
        case LESS_OR_EQUAL : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai <= where.data.datai); break;
                case 0 : return (data[target_attr].dataf <= where.data.dataf); break;
                default: return (data[target_attr].datas <= where.data.datas); break;
            }
        } break;
        case EQUAL : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai == where.data.datai); break;
                case 0 : return (data[target_attr].dataf == where.data.dataf); break;
                default: return (data[target_attr].datas == where.data.datas); break;
            }
        } break;
        case GREATER_OR_EQUAL : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai >= where.data.datai); break;
                case 0 : return (data[target_attr].dataf >= where.data.dataf); break;
                default: return (data[target_attr].datas >= where.data.datas); break;
            }
        } break;
        case GREATER : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai > where.data.datai); break;
                case 0 : return (data[target_attr].dataf > where.data.dataf); break;
                default: return (data[target_attr].datas > where.data.datas); break;
            }
        } break;
        case NOT_EQUAL : {
            switch (where.data.type) {
                case -1 : return (data[target_attr].datai != where.data.datai); break;
                case 0 : return (data[target_attr].dataf != where.data.dataf); break;
                default: return (data[target_attr].datas != where.data.datas); break;
            }
        } break;
        default:break;
    }
            
    return false;
}
