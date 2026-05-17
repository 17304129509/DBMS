

// catalog_manager.cpp - 目录管理器
// 负责管理数据库的元信息（catalog），包括表定义、属性定义、索引定义等。
// catalog信息以文本格式存储在页面缓冲区中，每条记录以'#'作为块结束标记。
//
// catalog文件格式（每条记录一行）：
//   <长度4字节> <表名> <属性数2字节> <属性1类型3字节> <属性1名> <属性1唯一1字节> ... <主键2字节> ;<索引数2字节> <索引1位置2字节> <索引1名> ...
//   每条记录以换行符和'#'结尾

#include "catalog_manager.h"
#include <direct.h>
#include <io.h>

// 根据current_database返回对应的catalog文件路径
// 当current_database为空时使用默认路径，否则使用数据库专属路径
std::string CatalogManager::getCatalogPath() {
    if (current_database.empty()) {
        return TABLE_MANAGER_PATH;
    }
    return "./database/" + current_database + "/catalog/catalog_file";
}

// 检查数据库目录是否已存在
// 通过_access系统调用检查目录是否存在
bool CatalogManager::hasDatabase(std::string db_name) {
    std::string path = "./database/" + db_name;
    return (_access(path.c_str(), 0) == 0);
}

// 创建表：将表的元信息写入catalog文件
// 格式：长度 + 表名 + 属性数量 + 各属性(类型+名称+唯一性) + 主键 + 索引数量 + 各索引(位置+名称)
void CatalogManager::createTable(std::string name, Attribute Attr, int primary, Index index){
    if(hasTable(name)){
        throw table_exist();
    }
    // 确保主键为unique（仅当primary_key有效时才设置，防止越界访问）
    if (primary >= 0 && primary < Attr.num)
        Attr.unique[primary]=true;
    // 记录每条信息的字符数（包括这里的5个）
    std::string str_tmp="0000 ";
    // 添加name
    str_tmp+=name;
    // 添加attribute的数量
    str_tmp=str_tmp+" "+num2str(Attr.num, 2);
    // 添加每个attribute的信息，顺序为类型，名字，是否为唯一
    for(int i=0;i<Attr.num;i++)
        str_tmp=str_tmp+" "+num2str(Attr.type[i], 3)+" "+Attr.name[i]+" "+(Attr.unique[i]==true?"1":"0");
    // 添加主键信息
    str_tmp=str_tmp+" "+num2str(primary, 2);
    // 添加index的数量, ;用来做标记index的开始
    str_tmp=str_tmp+" ;"+num2str(index.num, 2);
    // 添加index的信息，顺序为相对位置和名字
    for(int i=0;i<index.num;i++)
        str_tmp=str_tmp+" "+num2str(index.location[i], 2)+" "+index.indexname[i];
    // 换行后在结尾接上一个#，每个块以#结尾
    str_tmp=str_tmp+"\n"+"#";
    // 更改每条信息的长度的记录
    std::string str_len=num2str((int)str_tmp.length()-1, 4);
    str_tmp=str_len+str_tmp.substr(4,str_tmp.length()-4);
    // 计算块的数量
    int block_num=getBlockNum(getCatalogPath())/PAGESIZE;
    // 处理当块的数量为0的特殊情况
    if(block_num<=0)
        block_num=1;
    // 遍历所有的块，找到有足够空间的块插入
    for(int current_block=0;current_block<block_num;current_block++){
        char* buffer = buffer_manager.getPage(getCatalogPath() , current_block);
        int page_id = buffer_manager.getPageId(getCatalogPath() , current_block);
        // 寻找该block的有效长度
        int length=0;
        for(length=0;length<PAGESIZE&&buffer[length]!='\0'&&buffer[length]!='#';length++){}
        // 确保插入新信息后该页长度不会超过PAGESIZE
        if(length+(int)str_tmp.length()<PAGESIZE){
            // 删除末尾的#标记
            if(length&&buffer[length-1]=='#')
                buffer[length-1]='\0';
            else if(buffer[length]=='#')
                buffer[length]='\0';
            // 字符串拼接
            strcat_s(buffer, PAGESIZE, str_tmp.c_str());
            // 保存并刷新该页后返回
            buffer_manager.modifyPage(page_id);
            return;
        }
    }
    // 如果之前的块不够用，就新建一块后直接把信息插入
    char* buffer = buffer_manager.getPage(getCatalogPath() , block_num);
    int page_id = buffer_manager.getPageId(getCatalogPath() , block_num);
    strcat_s(buffer, PAGESIZE, str_tmp.c_str());
    buffer_manager.modifyPage(page_id);
    
}

// 删除表：从catalog文件中移除指定表的元信息
// 通过将非目标表的数据前移来覆盖目标表的记录
void CatalogManager::dropTable(std::string name){
    if(!hasTable(name)){
        throw table_not_exist();
    }
    // 寻找table_name所对应的块号和在该块的位置
    int suitable_block;
    int start_index=getTablePlace(name,suitable_block);
    // 得到所对应块的信息
    char* buffer = buffer_manager.getPage(getCatalogPath() , suitable_block);
    int page_id = buffer_manager.getPageId(getCatalogPath() , suitable_block);
    std::string buffer_check(buffer);
    // 求出应删除的块的index的开始和结尾后删除
    int end_index=start_index+str2num(buffer_check.substr(start_index,4));
    int index=0,current_index=0;;
    do{
        if(index<start_index||index>=end_index)
            buffer[current_index++]=buffer[index];
        index++;
    }while(buffer[index]!='#');
    buffer[current_index++]='#';
    buffer[current_index]='\0';
    // 刷新页面
    buffer_manager.modifyPage(page_id);
}

// 获取表的属性信息
// 从catalog文件中解析出指定表的所有属性定义
Attribute CatalogManager::getAttribute(std::string name){
    if(!hasTable(name)){
        throw attribute_not_exist();
    }
    // 寻找table_name对应的块和在块中的位置
    int suitable_block;
    int start_index=getTablePlace(name,suitable_block);
    // 得到所对应的块的信息
    char* buffer = buffer_manager.getPage(getCatalogPath() , suitable_block);
    std::string buffer_check(buffer);
    // end_index记录该行中表的名字的最后一个字符的位置
    int end_index=0;
    std::string table_name=getTableName(buffer_check, start_index, end_index);
    Attribute table_attr;
    start_index=end_index+1;
    // 得到attribute数量的字符串
    std::string attr_num=buffer_check.substr(start_index,2);
    table_attr.num=str2num(attr_num);
    start_index+=3;
    // 逐个解析属性的类型、名称和唯一性
    for(int index=0;index<table_attr.num;index++){
        // 对所有的attribute的类型和名字
        if(buffer_check[start_index]=='-'){
            // int类型：存储为-1
            table_attr.type[index]=-1;
            start_index+=5;
            while(buffer_check[start_index]!=' '){
                table_attr.name[index]+=buffer_check[start_index++];
            }
            start_index+=1;
            table_attr.unique[index]=(buffer_check[start_index]=='1'?true:false);
        }
        else if(str2num(buffer_check.substr(start_index,3))==0){
            // float类型：存储为0
            table_attr.type[index]=0;
            start_index+=4;
            while(buffer_check[start_index]!=' '){
                table_attr.name[index]+=buffer_check[start_index++];
            }
            start_index+=1;
            table_attr.unique[index]=(buffer_check[start_index]=='1'?true:false);
        }
        else{
            // char(n)类型：存储为n+1（+1以区分float的0）
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
    // 记录primary_key的信息
    if(buffer_check[start_index]=='-')
        table_attr.primary_key=-1;
    else
        table_attr.primary_key=str2num(buffer_check.substr(start_index,2));
    // 设置index的信息
    Index index_record=getIndex(table_name);
    for(int i=0;i<32;i++)
        table_attr.has_index[i]=false;
    for(int i=0;i<index_record.num;i++)
        table_attr.has_index[index_record.location[i]]=true;
    
    return table_attr;
}

// 检查表中是否存在指定属性
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

// 通过索引名找到对应的属性名
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

// 创建索引：在catalog中注册索引信息
// 通过dropTable+createTable的方式更新catalog记录
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
    // 找到属性在属性表中的位置作为索引位置
    for(int index=0;index<find_attr.num;index++){
        if(attr_name==find_attr.name[index])
        {
            index_record.location[index_record.num]=index;
            break;
        }
    }
    index_record.num++;
    // 删除旧catalog条目后重新插入（包含新的索引信息）
    dropTable(table_name);
    createTable(table_name, find_attr, find_attr.primary_key, index_record);
}

// 删除索引：从catalog中移除索引信息
// 通过与最后一个索引交换位置的方式高效删除
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
    // 通过将该信息与最后位置的索引替换的方式来删除索引
    index_record.indexname[hasindex]=index_record.indexname[index_record.num-1];
    index_record.location[hasindex]=index_record.location[index_record.num-1];
    index_record.num--;
    dropTable(table_name);
    createTable(table_name, attr_record, attr_record.primary_key, index_record);
    
}

// 显示表的结构信息（属性、类型、索引等）
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
                type="char("+std::to_string(attr_record.type[index]-1)+")";
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

// 检查表是否存在：遍历catalog文件中的所有块查找表名
bool CatalogManager::hasTable(std::string table_name){
    int block_num=getBlockNum(getCatalogPath())/PAGESIZE;
    if(block_num<=0)
        block_num=1;
    for(int current_block=0;current_block<block_num;current_block++){
        char* buffer = buffer_manager.getPage(getCatalogPath() , current_block);
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

// 数字转指定宽度的字符串（用于catalog记录的固定宽度字段）
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

// 字符串转数字
int CatalogManager::str2num(std::string str){
    return atoi(str.c_str());
}

// 从catalog记录中提取表名
// start为记录起始位置，rear返回表名结束位置
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

// 获取指定表在catalog文件中的块号和起始位置
int CatalogManager::getTablePlace(std::string name,int &suitable_block){
    int block_num=getBlockNum(getCatalogPath());
    if(block_num<=0)
        block_num=1;
    for(suitable_block=0;suitable_block<block_num;suitable_block++){
        char* buffer = buffer_manager.getPage(getCatalogPath() , suitable_block);
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

// 获取指定表的索引信息
// 从catalog记录中';'标记后解析索引数据
Index CatalogManager::getIndex(std::string table_name){
    Index index_record;
    int suitable_block;
    int start_index=getTablePlace(table_name,suitable_block);
    char* buffer = buffer_manager.getPage(getCatalogPath() , suitable_block);
    std::string buffer_check(buffer);
    // 跳到索引信息部分（以';'为分隔符）
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

// 获取文件的总块数
int CatalogManager::getBlockNum(std::string table_name) {
    char* p;
    int block_num = -1;
    do {
        p = buffer_manager.getPage(table_name , block_num + 1);
        block_num++;
    } while(p[0] != '\0');
    return block_num;
}

// ===== ALTER TABLE 相关方法实现 =====
// 所有ALTER操作都采用 dropTable + createTable 的方式更新catalog
// 因为catalog记录是固定格式的文本，不支持原地修改属性数量

// 为表添加一个新属性
// 在属性列表末尾追加新属性，然后重建catalog条目
void CatalogManager::addAttribute(std::string table_name, std::string attr_name, short type, bool unique) {
    if (!hasTable(table_name))
        throw table_not_exist();
    if (hasAttribute(table_name, attr_name))
        throw attribute_exist();

    Attribute attr = getAttribute(table_name);
    Index index_record = getIndex(table_name);

    // 在末尾添加新属性
    attr.name[attr.num] = attr_name;
    attr.type[attr.num] = type;
    attr.unique[attr.num] = unique;
    attr.has_index[attr.num] = false;
    attr.num++;

    // 删除旧catalog条目后重新插入
    dropTable(table_name);
    createTable(table_name, attr, attr.primary_key, index_record);
}

// 删除表中的一个属性
// 需要同时更新索引位置信息，因为属性位置变化会影响索引的location字段
void CatalogManager::dropAttribute(std::string table_name, std::string attr_name) {
    if (!hasTable(table_name))
        throw table_not_exist();
    if (!hasAttribute(table_name, attr_name))
        throw attribute_not_exist();

    Attribute attr = getAttribute(table_name);
    Index index_record = getIndex(table_name);

    // 找到要删除的属性的位置
    int attr_index = -1;
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == attr_name) {
            attr_index = i;
            break;
        }
    }

    // 不允许删除主键属性
    if (attr_index == attr.primary_key)
        throw primary_key_conflict();

    // 至少保留一个属性
    if (attr.num <= 1)
        throw input_format_error();

    // 前移属性数组，覆盖被删除的属性
    for (int i = attr_index; i < attr.num - 1; i++) {
        attr.name[i] = attr.name[i + 1];
        attr.type[i] = attr.type[i + 1];
        attr.unique[i] = attr.unique[i + 1];
        attr.has_index[i] = attr.has_index[i + 1];
    }
    attr.num--;

    // 更新主键位置索引（如果主键在被删属性之后，位置减1）
    if (attr.primary_key > attr_index)
        attr.primary_key--;

    // 更新索引位置：删除引用被删属性的索引，调整其他索引位置
    for (int i = 0; i < index_record.num; i++) {
        if (index_record.location[i] == attr_index) {
            // 用最后一个索引替换当前位置来删除
            index_record.indexname[i] = index_record.indexname[index_record.num - 1];
            index_record.location[i] = index_record.location[index_record.num - 1];
            index_record.num--;
            i--;
        } else if (index_record.location[i] > attr_index) {
            // 位置在被删属性之后的索引，位置减1
            index_record.location[i]--;
        }
    }

    // 删除旧catalog条目后重新插入
    dropTable(table_name);
    createTable(table_name, attr, attr.primary_key, index_record);
}

// 修改表中一个属性的类型和唯一约束
// 直接修改目标属性的类型和unique标志，然后重建catalog条目
void CatalogManager::modifyAttribute(std::string table_name, std::string attr_name, short new_type, bool new_unique) {
    if (!hasTable(table_name))
        throw table_not_exist();
    if (!hasAttribute(table_name, attr_name))
        throw attribute_not_exist();

    Attribute attr = getAttribute(table_name);
    Index index_record = getIndex(table_name);

    // 找到并修改目标属性
    for (int i = 0; i < attr.num; i++) {
        if (attr.name[i] == attr_name) {
            attr.type[i] = new_type;
            attr.unique[i] = new_unique;
            break;
        }
    }

    // 删除旧catalog条目后重新插入
    dropTable(table_name);
    createTable(table_name, attr, attr.primary_key, index_record);
}
