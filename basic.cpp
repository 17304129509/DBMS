#include "basic.h"

// 拷贝构造函数
Tuple::Tuple(const Tuple &tuple_in){
    for(int index=0;index<tuple_in.data_.size();index++)
    {
        this->data_.push_back(tuple_in.data_[index]);
    }
}

// 获取元组的数据项个数
inline int Tuple::getSize(){
    return (int)data_.size();
}

// 向元组中添加一个数据项
void Tuple::addData(Data data_in){
    this->data_.push_back(data_in);
}

// 判断元组是否被标记删除
bool Tuple::isDeleted() {
    return isDeleted_;
}

// 将元组标记为已删除
void Tuple::setDeleted() {
    isDeleted_ = true;
}

// 获取元组内所有数据
std::vector<Data> Tuple::getData() const{
    return this->data_;
}

// 打印输出元组内容
void Tuple::showTuple(){
    for(int index=0;index<getSize();index++){
        if(data_[index].type==-1)
            std::cout<<data_[index].datai<<'\t';
        else if(data_[index].type==0)
            std::cout<<data_[index].dataf<<'\t';
        else
            std::cout<<data_[index].datas<<'\t';
    }
    std::cout<<std::endl;
}

// 表结构构造函数，初始化表名和属性
Table::Table(std::string title,Attribute attr){
    this->title_=title;
    this->attr_=attr;
    this->index_.num=0;
}

// 表拷贝构造函数
Table::Table(const Table &table_in){
    this->attr_=table_in.attr_;
    this->index_=table_in.index_;
    this->title_=table_in.title_;
    for(int index=0;index<table_in.tuple_.size();index++)
        this->tuple_.push_back(table_in.tuple_[index]);
}

// 为表中指定属性建立索引
int Table::setIndex(short index,std::string index_name){
    short tmpIndex;
    for(tmpIndex=0;tmpIndex<index_.num;tmpIndex++){
        if(index==index_.location[tmpIndex])
        {
            std::cout<<"Illegal Set Index: The index has been in the table."<<std::endl;
            return 0;
        }
    }
    for(tmpIndex=0;tmpIndex<index_.num;tmpIndex++){
        if(index_name==index_.indexname[tmpIndex])
        {
            std::cout<<"Illegal Set Index: The name has been used."<<std::endl;
            return 0;
        }
    }
    index_.location[index_.num]=index;
    index_.indexname[index_.num]=index_name;
    index_.num++;
    return 1;
}

// 删除表中指定名称的索引
int Table::dropIndex(std::string index_name){
    short tmpIndex;
    for(tmpIndex=0;tmpIndex<index_.num;tmpIndex++){
        if(index_name==index_.indexname[tmpIndex])
            break;
    }
    if(tmpIndex==index_.num)
    {
        std::cout<<"Illegal Drop Index: No such a index in the table."<<std::endl;
        return 0;
    }

    index_.indexname[tmpIndex]=index_.indexname[index_.num-1];
    index_.location[tmpIndex]=index_.location[index_.num-1];
    index_.num--;
    return 1;
}

// 获取表名
std::string Table::getTitle(){
    return title_;
}

// 获取表的属性结构
Attribute Table::getAttr(){
    return attr_;
}

// 获取表中所有元组
std::vector<Tuple>& Table::getTuple(){
    return tuple_;
}

// 获取表的索引信息
Index Table::getIndex(){
    return index_;
}

// 打印输出整张表
void Table::showTable(){
    for(int index=0;index<attr_.num;index++)
        std::cout<<attr_.name[index]<<'\t';
    std::cout<<std::endl;
    for(int index=0;index<tuple_.size();index++)
        tuple_[index].showTuple();
}

// 限制行数打印输出表
void Table::showTable(int limit) {
    for(int index=0;index<attr_.num;index++)
        std::cout<<attr_.name[index]<<'\t';
    std::cout<<std::endl;
    for(int index=0;index<limit&&index<tuple_.size();index++)
        tuple_[index].showTuple();
}
