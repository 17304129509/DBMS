#include "catalog_manager.h"

// 创建数据表，将表结构、属性、主键和索引信息写入目录文件
void CatalogManager::createTable(std::string name, Attribute Attr, int primary, Index index){
    if(hasTable(name)){
        throw table_exist();
    }
    Attr.unique[primary]=true;
    std::string str_tmp="0000 ";
    str_tmp+=name;
    str_tmp=str_tmp+" "+num2str(Attr.num, 2);
    for(int i=0;i<Attr.num;i++)
        str_tmp=str_tmp+" "+num2str(Attr.type[i], 3)+" "+Attr.name[i]+" "+(Attr.unique[i]==true?"1":"0");
    str_tmp=str_tmp+" "+num2str(primary, 2);
    str_tmp=str_tmp+" ;"+num2str(index.num, 2);
    for(int i=0;i<index.num;i++)
        str_tmp=str_tmp+" "+num2str(index.location[i], 2)+" "+index.indexname[i];
    str_tmp=str_tmp+"\n"+"#";
    std::string str_len=num2str((int)str_tmp.length()-1, 4);
    str_tmp=str_len+str_tmp.substr(4,str_tmp.length()-4);
    int block_num=getBlockNum(TABLE_MANAGER_PATH)/PAGESIZE;
    if(block_num<=0)
        block_num=1;
    for(int current_block=0;current_block<block_num;current_block++){
        char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , current_block);
        int page_id = buffer_manager.getPageId(TABLE_MANAGER_PATH , current_block);
        int length=0;
        for(length=0;length<PAGESIZE&&buffer[length]!='\0'&&buffer[length]!='#';length++){}
        if(length+(int)str_tmp.length()<PAGESIZE){
            if(length&&buffer[length-1]=='#')
                buffer[length-1]='\0';
            else if(buffer[length]=='#')
                buffer[length]='\0';
            strcat_s(buffer, PAGESIZE, str_tmp.c_str());
            buffer_manager.modifyPage(page_id);
            return;
        }
    }
    char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , block_num);
    int page_id = buffer_manager.getPageId(TABLE_MANAGER_PATH , block_num);
    strcat_s(buffer, PAGESIZE, str_tmp.c_str());
    buffer_manager.modifyPage(page_id);
    
}

// 删除指定数据表，同步清理目录记录
void CatalogManager::dropTable(std::string name){
    if(!hasTable(name)){
        throw table_not_exist();
    }
    int suitable_block;
    int start_index=getTablePlace(name,suitable_block);
    char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , suitable_block);
    int page_id = buffer_manager.getPageId(TABLE_MANAGER_PATH , suitable_block);
    std::string buffer_check(buffer);
    int end_index=start_index+str2num(buffer_check.substr(start_index,4));
    int index=0,current_index=0;;
    do{
        if(index<start_index||index>=end_index)
            buffer[current_index++]=buffer[index];
        index++;
    }while(buffer[index]!='#');
    buffer[current_index++]='#';
    buffer[current_index]='\0';
    buffer_manager.modifyPage(page_id);
}

// 读取并返回目标表的全部属性信息
Attribute CatalogManager::getAttribute(std::string name){
    if(!hasTable(name)){
        throw attribute_not_exist();
    }
    int suitable_block;
    int start_index=getTablePlace(name,suitable_block);
    char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , suitable_block);
    std::string buffer_check(buffer);
    int end_index=0;
    std::string table_name=getTableName(buffer_check, start_index, end_index);
    Attribute table_attr;
    start_index=end_index+1;
    std::string attr_num=buffer_check.substr(start_index,2);
    table_attr.num=str2num(attr_num);
    start_index+=3;
    for(int index=0;index<table_attr.num;index++){
        if(buffer_check[start_index]=='-'){
            table_attr.type[index]=-1;
            start_index+=5;
            while(buffer_check[start_index]!=' '){
                table_attr.name[index]+=buffer_check[start_index++];
            }
            start_index+=1;
            table_attr.unique[index]=(buffer_check[start_index]=='1'?true:false);
        }
        else if(str2num(buffer_check.substr(start_index,3))==0){
            table_attr.type[index]=0;
            start_index+=4;
            while(buffer_check[start_index]!=' '){
                table_attr.name[index]+=buffer_check[start_index++];
            }
            start_index+=1;
            table_attr.unique[index]=(buffer_check[start_index]=='1'?true:false);
        }
        else{
            table_attr.type[index]=str2num(buffer_check.substr(start_index,3));
            start_index+=4;
            while(buffer_check[start_index]!=' '){
                table_attr.name[index]+=buffer_check[start_index++];
            }
            start_index+=1;
            table_attr.unique[index]=(buffer_check[start_index]=='1'?true:false);
        }
        start_index+=2;
    }
    if(buffer_check[start_index]=='-')
        table_attr.primary_key=-1;
    else
        table_attr.primary_key=str2num(buffer_check.substr(start_index,2));
    Index index_record=getIndex(table_name);
    for(int i=0;i<32;i++)
        table_attr.has_index[i]=false;
    for(int i=0;i<index_record.num;i++)
        table_attr.has_index[index_record.location[i]]=true;
    
    return table_attr;
}

// 检测目标表中是否存在对应属性
bool CatalogManager::hasAttribute(std::string table_name , std::string attr_name){
    if(!hasTable(table_name)){
        throw table_not_exist();
    }
    Attribute find_attr=getAttribute(table_name);
    for(int index=0;index<find_attr.num;index++){
        if(attr_name==find_attr.name[index])
            return true;
    }
    return false;
}

// 通过索引名称反向查找所属属性名
std::string CatalogManager::IndextoAttr(std::string table_name, std::string index_name){
    if(!hasTable(table_name))
        throw table_not_exist();
    Index index_record=getIndex(table_name);
    int hasfind=-1;
    for(int i=0;i<index_record.num;i++){
        if(index_record.indexname[i]==index_name){
            hasfind=i;
            break;
        }
    }
    if(hasfind==-1)
        throw index_not_exist();
    Attribute attr_record=getAttribute(table_name);
    return attr_record.name[index_record.location[hasfind]];
}

// 为数据表指定字段新建索引
void CatalogManager::createIndex(std::string table_name,std::string attr_name,std::string index_name){
    if(!hasTable(table_name))
        throw table_not_exist();
    if(!hasAttribute(table_name, attr_name))
        throw attribute_not_exist();
    Index index_record=getIndex(table_name);
    if(index_record.num>=10)
        throw index_full();
    Attribute find_attr=getAttribute(table_name);
    for(int i=0;i<index_record.num;i++){
        if(index_record.indexname[i]==index_name)
            throw index_exist();
        if(find_attr.name[index_record.location[i]]==attr_name)
            throw index_exist();
    }
    index_record.indexname[index_record.num]=index_name;
    for(int index=0;index<find_attr.num;index++){
        if(attr_name==find_attr.name[index])
        {
            index_record.location[index_record.num]=index;
            break;
        }
    }
    index_record.num++;
    dropTable(table_name);
    createTable(table_name, find_attr, find_attr.primary_key, index_record);
}

// 移除数据表中指定的索引
void CatalogManager::dropIndex(std::string table_name,std::string index_name){
    if(!hasTable(table_name)){
        throw table_not_exist();
    }
    Index index_record=getIndex(table_name);
    Attribute attr_record=getAttribute(table_name);
    int hasindex=-1;
    for(int index=0;index<index_record.num;index++){
        if(index_record.indexname[index]==index_name){
            hasindex=index;
            break;
        }
    }
    if(hasindex==-1){
        throw index_not_exist();
    }
    index_record.indexname[hasindex]=index_record.indexname[index_record.num-1];
    index_record.location[hasindex]=index_record.location[index_record.num-1];
    index_record.num--;
    dropTable(table_name);
    createTable(table_name, attr_record, attr_record.primary_key, index_record);
    
}

// 格式化输出表的属性与索引全部信息
void CatalogManager::showTable(std::string table_name){
    if(!hasTable(table_name)){
        throw table_not_exist();
    }
    std::cout<<"Table name:"<<table_name<<std::endl;
    Attribute attr_record=getAttribute(table_name);
    Index index_record=getIndex(table_name);
    int longest=-1;
    for(int index=0;index<attr_record.num;index++){
        if((int)attr_record.name[index].length()>longest)
            longest=(int)attr_record.name[index].length();
    }
    std::string type;
    std::cout<<"Attribute:"<<std::endl;
    std::cout<<"Num|"<<"Name"<<std::setw(longest+2)<<"|Type"<<type<<std::setw(6)<<"|"<<"Unique|Primary Key"<<std::endl;
    for(int index_out=0;index_out<longest+35;index_out++)
        std::cout<<"-";
    std::cout<<std::endl;
    for(int index=0;index<attr_record.num;index++){
        switch (attr_record.type[index]) {
            case -1:
                type="int";
                break;
            case 0:
                type="float";
                break;
            default:
                type="char("+num2str(attr_record.type[index]-1, 3)+")";
                break;
        }
        std::cout<<index<<std::setw(3-index/10)<<"|"<<attr_record.name[index]<<std::setw(longest-(int)attr_record.name[index].length()+2)<<"|"<<type<<std::setw(10-(int)type.length())<<"|";
        if(attr_record.unique[index])
            std::cout<<"unique"<<"|";
        else
            std::cout<<std::setw(7)<<"|";
        if(attr_record.primary_key==index)
            std::cout<<"primary key";
        std::cout<<std::endl;
    }
    
    for(int index_out=0;index_out<longest+35;index_out++)
        std::cout<<"-";
    
    std::cout<<std::endl;
    
    std::cout<<"Index:"<<std::endl;
    std::cout<<"Num|Location|Name"<<std::endl;
    longest=-1;
    for(int index_out=0;index_out<index_record.num;index_out++){
        if((int)index_record.indexname[index_out].length()>longest)
            longest=(int)index_record.indexname[index_out].length();
    }
    for(int index_out=0;index_out<((longest+14)>18?(longest+14):18);index_out++)
        std::cout<<"-";
    std::cout<<std::endl;
    for(int index_out=0;index_out<index_record.num;index_out++){
        std::cout<<index_out<<std::setw(3-index_out/10)<<"|"<<index_record.location[index_out]<<std::setw(8-index_record.location[index_out]/10)<<"|"<<index_record.indexname[index_out]<<std::endl;
    }
    for(int index_out=0;index_out<((longest+14)>18?(longest+14):18);index_out++)
        std::cout<<"-";
    std::cout<<std::endl<<std::endl;
}

// 遍历目录文件，检测目标表是否存在
bool CatalogManager::hasTable(std::string table_name){
    int block_num=getBlockNum(TABLE_MANAGER_PATH)/PAGESIZE;
    if(block_num<=0)
        block_num=1;
    for(int current_block=0;current_block<block_num;current_block++){
        char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , current_block);
        std::string buffer_check(buffer);
        std::string str_tmp="";
        int start_index=0,end_index=0;
        do{
            if(buffer_check[0]=='#')
                break;
            else if(getTableName(buffer, start_index, end_index)==table_name){
                return true;
            }
            else{
                start_index+=str2num(buffer_check.substr(start_index,4));
                if(!start_index)
                    break;
            }
        }while(buffer_check[start_index]!='#');
    }
    return false;
}

// 将整型数字转为固定位数字符串
std::string CatalogManager::num2str(int num,short bit){
    std::string str="";
    if(num<0){
        num=-num;
        str+="-";
    }
    int divisor=pow(10,bit-1);
    for(int i=0;i<bit;i++){
        str+=(num/divisor%10+'0');
        divisor/=10;
    }
    return str;
}

// 字符串转为整型数字
int CatalogManager::str2num(std::string str){
    return atoi(str.c_str());
}

// 从目录缓存中截取解析表名
std::string CatalogManager::getTableName(std::string buffer,int start,int &rear){
    std::string str_tmp="";
    rear=0;
    if(buffer=="")
        return buffer;
    while(buffer[start+rear+5]!=' '){
        rear++;
    }
    str_tmp=buffer.substr(start+5,rear);
    rear=start+5+rear;
    return str_tmp;
}

// 检索表所在磁盘块与偏移位置
int CatalogManager::getTablePlace(std::string name,int &suitable_block){
    int block_num=getBlockNum(TABLE_MANAGER_PATH);
    if(block_num<=0)
        block_num=1;
    for(suitable_block=0;suitable_block<block_num;suitable_block++){
        char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , suitable_block);
        std::string buffer_check(buffer);
        std::string str_tmp="";
        int start=0,rear=0;
        do{
            if(buffer_check[0]=='#')
                break;
            if(getTableName(buffer, start, rear)==name){
                return start;
            }
            else{
                start+=str2num(buffer_check.substr(start,4));
                if(!start)
                    break;
            }
        }while(buffer_check[start]!='#');
    }
    return -1;
}

// 读取目标表保存的所有索引信息
Index CatalogManager::getIndex(std::string table_name){
    Index index_record;
    int suitable_block;
    int start_index=getTablePlace(table_name,suitable_block);
    char* buffer = buffer_manager.getPage(TABLE_MANAGER_PATH , suitable_block);
    std::string buffer_check(buffer);
    while(buffer_check[start_index]!=';')
        start_index++;
    start_index++;
    index_record.num=str2num(buffer_check.substr(start_index,2));
    for(int times=0;times<index_record.num;times++){
        start_index+=3;
        index_record.location[times]=str2num(buffer_check.substr(start_index,2));
        start_index+=3;
        while(buffer_check[start_index]!=' '&&buffer_check[start_index]!='#'&&buffer_check[start_index]!='\n'){
            index_record.indexname[times]+=buffer_check[start_index++];
        }
        start_index-=2;
    }
    return index_record;
}

// 统计目录文件占用的数据块总数
int CatalogManager::getBlockNum(std::string table_name) {
    char* p;
    int block_num = -1;
    do {
        p = buffer_manager.getPage(table_name , block_num + 1);
        block_num++;
    } while(p[0] != '\0');
    return block_num;
}
