// mainwindow.cpp - MiniSQL GUI主窗口实现
// 实现了所有UI控件的创建、布局和事件处理逻辑。
//
// 核心设计思路：
// - 所有GUI操作（添加字段、编辑数据等）最终都转化为SQL语句，
//   通过Interpreter执行，保证GUI操作和命令行操作的一致性
// - 左侧数据库树通过读取文件系统目录结构构建
// - 字段信息和数据表格通过CatalogManager和RecordManager获取

#include "mainwindow.h"
#include "interpreter.h"
#include "api.h"
#include "buffer_manager.h"
#include "const.h"
#include "catalog_manager.h"
#include <QSplitter>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QPushButton>
#include <QFont>
#include <QDir>
#include <QFileInfo>
#include <QKeySequence>
#include <QTableWidgetItem>

extern BufferManager buffer_manager;
extern std::string current_database;

// 构造函数：初始化UI和菜单
MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent), isUpdatingTable(false)
{
    setupUI();
    setupMenu();
}

MainWindow::~MainWindow()
{
}

// ===== 初始化UI布局 =====
// 整体结构：水平分割器（左树 | 右面板）
// 右面板：垂直分割器（上：输出+输入 | 下：字段管理+数据表格）
void MainWindow::setupUI()
{
    setWindowTitle("MiniSQL - Database Management System");
    resize(1200, 800);

    QSplitter *mainSplitter = new QSplitter(Qt::Horizontal, this);

    // ----- 左侧面板：数据库树 -----
    // 显示所有数据库→表→字段的三级树形结构
    // 点击表名时触发onTreeItemClicked，加载字段和数据
    QWidget *leftPanel = new QWidget;
    QVBoxLayout *leftLayout = new QVBoxLayout(leftPanel);
    leftLayout->setContentsMargins(4, 4, 4, 4);
    QLabel *treeLabel = new QLabel("Database Explorer");
    treeLabel->setStyleSheet("font-weight: bold; font-size: 13px;");
    leftLayout->addWidget(treeLabel);

    dbTree = new QTreeWidget;
    dbTree->setHeaderLabel("Databases");
    dbTree->setMinimumWidth(200);
    dbTree->setMaximumWidth(350);
    connect(dbTree, &QTreeWidget::itemClicked, this, &MainWindow::onTreeItemClicked);
    leftLayout->addWidget(dbTree);

    QPushButton *refreshBtn = new QPushButton("Refresh");
    connect(refreshBtn, &QPushButton::clicked, this, &MainWindow::refreshTree);
    leftLayout->addWidget(refreshBtn);

    mainSplitter->addWidget(leftPanel);

    // ----- 右侧面板 -----
    QSplitter *rightSplitter = new QSplitter(Qt::Vertical, this);

    // ===== 上半部分：输出区 + 输入区 =====
    QWidget *topWidget = new QWidget;
    QVBoxLayout *topLayout = new QVBoxLayout(topWidget);
    topLayout->setContentsMargins(4, 4, 4, 4);

    // 输出区：只读，显示SQL执行结果，深色背景模拟终端
    QLabel *outputLabel = new QLabel("Query Results");
    outputLabel->setStyleSheet("font-weight: bold; font-size: 13px;");
    topLayout->addWidget(outputLabel);

    outputArea = new QTextEdit;
    outputArea->setReadOnly(true);
    outputArea->setFont(QFont("Consolas", 10));
    outputArea->setStyleSheet("background-color: #1e1e1e; color: #d4d4d4;");
    outputArea->append("Welcome to MiniSQL GUI");
    topLayout->addWidget(outputArea, 1);

    // 输入区：多行文本框，支持粘贴多条SQL（按分号分割执行）
    QLabel *inputLabel = new QLabel("SQL Command (end with ;)");
    inputLabel->setStyleSheet("font-weight: bold; font-size: 13px;");
    topLayout->addWidget(inputLabel);

    QVBoxLayout *inputLayout = new QVBoxLayout;
    inputLine = new QPlainTextEdit;
    inputLine->setPlaceholderText("Enter SQL command here (e.g., SELECT * FROM student;)");
    inputLine->setFont(QFont("Consolas", 10));
    inputLine->setMaximumHeight(100);
    inputLayout->addWidget(inputLine);

    QPushButton *execBtn = new QPushButton("Execute (Ctrl+Enter)");
    execBtn->setMinimumHeight(28);
    execBtn->setStyleSheet("background-color: #0078d4; color: white; font-weight: bold; padding: 4px 16px;");
    connect(execBtn, &QPushButton::clicked, this, &MainWindow::executeQuery);
    inputLayout->addWidget(execBtn);

    topLayout->addLayout(inputLayout);
    rightSplitter->addWidget(topWidget);

    // ===== 下半部分：字段管理 + 数据表格 =====
    // 这两个区域只在点击左侧树中的表名后才有内容
    QWidget *bottomWidget = new QWidget;
    QVBoxLayout *bottomLayout = new QVBoxLayout(bottomWidget);
    bottomLayout->setContentsMargins(4, 4, 4, 4);

    // 当前选中表名标签
    currentTableLabel = new QLabel("No table selected");
    currentTableLabel->setStyleSheet("font-weight: bold; font-size: 13px; color: #0078d4;");
    bottomLayout->addWidget(currentTableLabel);

    // ----- 字段管理区域 -----
    // 左边是字段信息表格（不可编辑），右边是操作按钮
    // Add Field：弹出对话框，输入字段名+类型+是否UNIQUE → ALTER TABLE ADD
    // Drop Field：选中一行后点击 → ALTER TABLE DROP COLUMN
    // Modify Field：选中一行后点击，弹出对话框修改类型和约束 → ALTER TABLE MODIFY COLUMN
    QHBoxLayout *fieldLayout = new QHBoxLayout;
    QLabel *fieldLabel = new QLabel("Fields:");
    fieldLabel->setStyleSheet("font-weight: bold;");
    fieldLayout->addWidget(fieldLabel);

    fieldTableWidget = new QTableWidget;
    fieldTableWidget->setColumnCount(4);
    fieldTableWidget->setHorizontalHeaderLabels(QStringList() << "Name" << "Type" << "Unique" << "Primary Key");
    fieldTableWidget->horizontalHeader()->setStretchLastSection(true);
    fieldTableWidget->setSelectionBehavior(QAbstractItemView::SelectRows);
    fieldTableWidget->setMaximumHeight(120);
    fieldTableWidget->setEditTriggers(QAbstractItemView::NoEditTriggers);
    fieldLayout->addWidget(fieldTableWidget, 1);

    QVBoxLayout *fieldBtnLayout = new QVBoxLayout;
    QPushButton *addFieldBtn = new QPushButton("Add Field");
    addFieldBtn->setStyleSheet("padding: 4px 8px;");
    connect(addFieldBtn, &QPushButton::clicked, this, &MainWindow::addField);
    fieldBtnLayout->addWidget(addFieldBtn);

    QPushButton *dropFieldBtn = new QPushButton("Drop Field");
    dropFieldBtn->setStyleSheet("padding: 4px 8px;");
    connect(dropFieldBtn, &QPushButton::clicked, this, &MainWindow::dropField);
    fieldBtnLayout->addWidget(dropFieldBtn);

    QPushButton *modifyFieldBtn = new QPushButton("Modify Field");
    modifyFieldBtn->setStyleSheet("padding: 4px 8px;");
    connect(modifyFieldBtn, &QPushButton::clicked, this, &MainWindow::modifyField);
    fieldBtnLayout->addWidget(modifyFieldBtn);
    fieldBtnLayout->addStretch();
    fieldLayout->addLayout(fieldBtnLayout);

    bottomLayout->addLayout(fieldLayout);

    // ----- 数据表格区域 -----
    // 左边是数据表格（双击单元格可直接编辑），右边是操作按钮
    // Add Row：弹出对话框，为每个字段输入值 → INSERT INTO
    // Delete Row：选中一行后点击 → DELETE FROM WHERE
    // 直接编辑单元格：双击后修改值 → 自动生成UPDATE语句
    QHBoxLayout *dataLayout = new QHBoxLayout;
    QLabel *dataLabel = new QLabel("Data:");
    dataLabel->setStyleSheet("font-weight: bold;");
    dataLayout->addWidget(dataLabel);

    dataTableWidget = new QTableWidget;
    dataTableWidget->horizontalHeader()->setStretchLastSection(true);
    dataTableWidget->setSelectionBehavior(QAbstractItemView::SelectRows);
    connect(dataTableWidget, &QTableWidget::cellChanged, this, &MainWindow::onTableDataChanged);
    dataLayout->addWidget(dataTableWidget, 1);

    QVBoxLayout *dataBtnLayout = new QVBoxLayout;
    QPushButton *addRowBtn = new QPushButton("Add Row");
    addRowBtn->setStyleSheet("padding: 4px 8px; background-color: #28a745; color: white;");
    connect(addRowBtn, &QPushButton::clicked, this, &MainWindow::addRow);
    dataBtnLayout->addWidget(addRowBtn);

    QPushButton *deleteRowBtn = new QPushButton("Delete Row");
    deleteRowBtn->setStyleSheet("padding: 4px 8px; background-color: #dc3545; color: white;");
    connect(deleteRowBtn, &QPushButton::clicked, this, &MainWindow::deleteRow);
    dataBtnLayout->addWidget(deleteRowBtn);
    dataBtnLayout->addStretch();
    dataLayout->addLayout(dataBtnLayout);

    bottomLayout->addLayout(dataLayout);

    rightSplitter->addWidget(bottomWidget);
    // 上下比例3:2
    rightSplitter->setStretchFactor(0, 3);
    rightSplitter->setStretchFactor(1, 2);

    mainSplitter->addWidget(rightSplitter);
    // 左右比例1:3
    mainSplitter->setStretchFactor(0, 1);
    mainSplitter->setStretchFactor(1, 3);

    setCentralWidget(mainSplitter);
    statusBar()->showMessage("Ready - Click a table in the tree to view/edit data");
}

// ===== 初始化菜单栏 =====
void MainWindow::setupMenu()
{
    QMenuBar *menuBar = this->menuBar();

    QMenu *fileMenu = menuBar->addMenu("File");
    QAction *openScript = fileMenu->addAction("Open SQL Script...");
    connect(openScript, &QAction::triggered, this, &MainWindow::openScriptFile);
    fileMenu->addSeparator();
    QAction *exitAction = fileMenu->addAction("Exit");
    connect(exitAction, &QAction::triggered, this, &QWidget::close);

    QMenu *viewMenu = menuBar->addMenu("View");
    QAction *refreshAction = viewMenu->addAction("Refresh Database Tree");
    connect(refreshAction, &QAction::triggered, this, &MainWindow::refreshTree);

    QMenu *helpMenu = menuBar->addMenu("Help");
    QAction *aboutAction = helpMenu->addAction("About");
    connect(aboutAction, &QAction::triggered, this, &MainWindow::showAbout);
}

// ===== 点击树形结构中的项目 =====
// 判断点击的是表名还是字段名，找到对应的表后加载字段信息和数据
// 树的三级结构：数据库(无parent) → 表(有parent无grandparent) → 字段(有grandparent)
void MainWindow::onTreeItemClicked(QTreeWidgetItem *item, int column)
{
    if (!item) return;

    QTreeWidgetItem *parentItem = item->parent();
    // 点击的是数据库节点（最顶层），不做处理
    if (!parentItem) return;

    QTreeWidgetItem *grandParentItem = parentItem->parent();

    std::string tableName;
    if (grandParentItem) {
        // 点击的是字段节点，提取字段名中空格前的表名部分
        // 字段节点显示格式："fieldName (TYPE)"，需要去掉括号部分
        tableName = item->text(0).toStdString();
        int spacePos = tableName.find(' ');
        if (spacePos != std::string::npos)
            tableName = tableName.substr(0, spacePos);
    } else {
        // 点击的是表节点，直接获取表名
        QString text = item->text(0);
        tableName = text.toStdString();
    }

    if (tableName.empty()) return;

    // 验证表是否存在（字段名可能不是有效的表名）
    CatalogManager cm;
    if (!cm.hasTable(tableName)) return;

    // 记录当前选中的表，加载字段信息和数据
    currentTableName = tableName;
    currentTableLabel->setText("Table: " + QString::fromStdString(tableName));
    loadFieldInfo(tableName);
    loadTableData(tableName);
}

// ===== 加载字段信息到字段管理表格 =====
// 从CatalogManager读取表的属性定义和索引信息
// 通过索引信息判断哪个字段是主键
void MainWindow::loadFieldInfo(const std::string &tableName)
{
    CatalogManager cm;
    Attribute attr = cm.getAttribute(tableName);
    Index idx = cm.getIndex(tableName);
    // 通过索引位置找到主键字段的索引号
    int primary = -1;
    for (int i = 0; i < idx.num; i++) {
        if (idx.location[i] >= 0) {
            for (int j = 0; j < attr.num; j++) {
                if (j == idx.location[i] && attr.has_index[j]) {
                    primary = j;
                }
            }
        }
    }

    // 填充字段表格：名称、类型、是否UNIQUE、是否主键
    fieldTableWidget->setRowCount(attr.num);
    for (int i = 0; i < attr.num; i++) {
        fieldTableWidget->setItem(i, 0, new QTableWidgetItem(QString::fromStdString(attr.name[i])));
        QString typeStr;
        switch (attr.type[i]) {
            case -1: typeStr = "INT"; break;
            case 0: typeStr = "FLOAT"; break;
            default: typeStr = QString("CHAR(%1)").arg(attr.type[i] - 1); break;
        }
        fieldTableWidget->setItem(i, 1, new QTableWidgetItem(typeStr));
        fieldTableWidget->setItem(i, 2, new QTableWidgetItem(attr.unique[i] ? "Yes" : "No"));
        fieldTableWidget->setItem(i, 3, new QTableWidgetItem(i == primary ? "Yes" : "No"));
    }
    fieldTableWidget->resizeColumnsToContents();
}

// ===== 加载表数据到数据表格 =====
// 从RecordManager读取所有记录，过滤已删除的记录后显示
// isUpdatingTable标志防止加载数据时触发onTableDataChanged
void MainWindow::loadTableData(const std::string &tableName)
{
    isUpdatingTable = true;

    CatalogManager cm;
    Attribute attr = cm.getAttribute(tableName);
    RecordManager rm;
    Table table = rm.selectRecord(tableName);
    std::vector<Tuple> tuples = table.getTuple();

    // 设置列标题为字段名
    dataTableWidget->setColumnCount(attr.num);
    QStringList headers;
    for (int i = 0; i < attr.num; i++) {
        headers << QString::fromStdString(attr.name[i]);
    }
    dataTableWidget->setHorizontalHeaderLabels(headers);

    // 计算有效行数（排除已删除的记录）
    int validRows = 0;
    for (int i = 0; i < (int)tuples.size(); i++) {
        if (!tuples[i].isDeleted()) validRows++;
    }
    dataTableWidget->setRowCount(validRows);

    // 填充数据：根据字段类型将Data转为QString显示
    int row = 0;
    for (int i = 0; i < (int)tuples.size(); i++) {
        if (tuples[i].isDeleted()) continue;
        std::vector<Data> data = tuples[i].getData();
        for (int j = 0; j < (int)data.size() && j < attr.num; j++) {
            QString val;
            switch (data[j].type) {
                case -1: val = QString::number(data[j].datai); break;
                case 0: val = QString::number(data[j].dataf, 'f', 2); break;
                default: val = QString::fromStdString(data[j].datas); break;
            }
            QTableWidgetItem *item = new QTableWidgetItem(val);
            dataTableWidget->setItem(row, j, item);
        }
        row++;
    }
    dataTableWidget->resizeColumnsToContents();
    isUpdatingTable = false;
}

// ===== 数据表格单元格被编辑 =====
// 用户双击单元格修改值后，自动生成UPDATE语句执行
// WHERE条件使用第一个UNIQUE字段或第一个字段来定位行
void MainWindow::onTableDataChanged(int row, int col)
{
    // isUpdatingTable=true时是程序加载数据，不是用户编辑，跳过
    if (isUpdatingTable || currentTableName.empty()) return;

    QTableWidgetItem *item = dataTableWidget->item(row, col);
    if (!item) return;

    CatalogManager cm;
    Attribute attr = cm.getAttribute(currentTableName);
    std::string attrName = attr.name[col];
    QString newValue = item->text();

    // 根据字段类型决定值是否需要加引号（字符串类型需要）
    std::string valueStr;
    switch (attr.type[col]) {
        case -1:
        case 0:
            valueStr = newValue.toStdString();
            break;
        default:
            valueStr = "'" + newValue.toStdString() + "'";
            break;
    }

    // 构造WHERE条件：用UNIQUE字段或第一个字段定位要更新的行
    std::string whereAttrName;
    std::string whereValue;
    for (int i = 0; i < attr.num; i++) {
        if (attr.unique[i] || i == 0) {
            whereAttrName = attr.name[i];
            QTableWidgetItem *whereItem = dataTableWidget->item(row, i);
            if (whereItem) {
                switch (attr.type[i]) {
                    case -1:
                    case 0:
                        whereValue = whereItem->text().toStdString();
                        break;
                    default:
                        whereValue = "'" + whereItem->text().toStdString() + "'";
                        break;
                }
            }
            break;
        }
    }

    if (whereAttrName.empty()) return;

    // 生成并执行UPDATE语句
    std::string sql = "UPDATE " + currentTableName + " SET " + attrName + " = " + valueStr;
    sql += " WHERE " + whereAttrName + " = " + whereValue + ";";

    try {
        Interpreter interpreter;
        std::string result = interpreter.executeQuery(sql);
        if (!result.empty()) {
            QString qresult = QString::fromStdString(result);
            qresult.replace(">>> ", "");
            outputArea->append("Auto-update: " + qresult);
        }
    } catch (...) {
        outputArea->append("Error: Failed to update cell!");
        loadTableData(currentTableName);
    }
    refreshTree();
}

// ===== 添加一行数据 =====
// 弹出表单对话框，为每个字段生成一个输入框
// 用户填写后生成INSERT INTO ... VALUES (...)语句执行
void MainWindow::addRow()
{
    if (currentTableName.empty()) {
        QMessageBox::warning(this, "Warning", "Please select a table first!");
        return;
    }

    CatalogManager cm;
    Attribute attr = cm.getAttribute(currentTableName);

    // 创建表单对话框，每个字段一行输入
    QDialog dialog(this);
    dialog.setWindowTitle("Add Row to " + QString::fromStdString(currentTableName));
    QFormLayout form(&dialog);

    QVector<QLineEdit*> editors;
    for (int i = 0; i < attr.num; i++) {
        QString label = QString::fromStdString(attr.name[i]);
        QString typeHint;
        switch (attr.type[i]) {
            case -1: typeHint = "INT"; break;
            case 0: typeHint = "FLOAT"; break;
            default: typeHint = QString("CHAR(%1)").arg(attr.type[i] - 1); break;
        }
        QLineEdit *editor = new QLineEdit(&dialog);
        editor->setPlaceholderText(typeHint);
        form.addRow(label + " (" + typeHint + "):", editor);
        editors.push_back(editor);
    }

    QDialogButtonBox buttons(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    connect(&buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(&buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    form.addRow(&buttons);

    if (dialog.exec() == QDialog::Accepted) {
        // 根据用户输入构造INSERT语句
        std::string sql = "INSERT INTO " + currentTableName + " VALUES (";
        for (int i = 0; i < attr.num; i++) {
            QString val = editors[i]->text().trimmed();
            if (val.isEmpty()) {
                QMessageBox::warning(this, "Error", "All fields are required!");
                return;
            }
            switch (attr.type[i]) {
                case -1:
                case 0:
                    sql += val.toStdString();
                    break;
                default:
                    sql += "'" + val.toStdString() + "'";
                    break;
            }
            if (i < attr.num - 1) sql += ", ";
        }
        sql += ");";

        try {
            Interpreter interpreter;
            std::string result = interpreter.executeQuery(sql);
            if (!result.empty()) {
                QString qresult = QString::fromStdString(result);
                qresult.replace(">>> ", "");
                outputArea->append("Add row: " + qresult);
            }
        } catch (...) {
            outputArea->append("Error: Failed to add row!");
        }
        loadTableData(currentTableName);
        refreshTree();
    }
}

// ===== 删除一行数据 =====
// 选中数据表格中的一行，用UNIQUE字段或第一个字段构造WHERE条件
// 生成DELETE FROM ... WHERE ...语句执行
void MainWindow::deleteRow()
{
    if (currentTableName.empty()) {
        QMessageBox::warning(this, "Warning", "Please select a table first!");
        return;
    }

    int row = dataTableWidget->currentRow();
    if (row < 0) {
        QMessageBox::warning(this, "Warning", "Please select a row to delete!");
        return;
    }

    CatalogManager cm;
    Attribute attr = cm.getAttribute(currentTableName);

    // 用UNIQUE字段或第一个字段构造WHERE条件
    std::string whereAttrName;
    std::string whereValue;
    for (int i = 0; i < attr.num; i++) {
        if (attr.unique[i] || i == 0) {
            whereAttrName = attr.name[i];
            QTableWidgetItem *item = dataTableWidget->item(row, i);
            if (item) {
                switch (attr.type[i]) {
                    case -1:
                    case 0:
                        whereValue = item->text().toStdString();
                        break;
                    default:
                        whereValue = "'" + item->text().toStdString() + "'";
                        break;
                }
            }
            break;
        }
    }

    if (whereAttrName.empty()) return;

    // 确认删除
    if (QMessageBox::question(this, "Confirm Delete",
        "Delete this row?") != QMessageBox::Yes)
        return;

    std::string sql = "DELETE FROM " + currentTableName + " WHERE " + whereAttrName + " = " + whereValue + ";";

    try {
        Interpreter interpreter;
        std::string result = interpreter.executeQuery(sql);
        if (!result.empty()) {
            QString qresult = QString::fromStdString(result);
            qresult.replace(">>> ", "");
            outputArea->append("Delete row: " + qresult);
        }
    } catch (...) {
        outputArea->append("Error: Failed to delete row!");
    }
    loadTableData(currentTableName);
    refreshTree();
}

// ===== 添加字段 =====
// 弹出对话框输入字段名、类型、是否UNIQUE
// 生成 ALTER TABLE ... ADD ...语句执行
void MainWindow::addField()
{
    if (currentTableName.empty()) {
        QMessageBox::warning(this, "Warning", "Please select a table first!");
        return;
    }

    QDialog dialog(this);
    dialog.setWindowTitle("Add Field to " + QString::fromStdString(currentTableName));
    QFormLayout form(&dialog);

    QLineEdit *nameEdit = new QLineEdit(&dialog);
    form.addRow("Field Name:", nameEdit);

    QComboBox *typeCombo = new QComboBox(&dialog);
    typeCombo->addItems(QStringList() << "INT" << "FLOAT" << "CHAR(10)" << "CHAR(20)" << "CHAR(50)");
    form.addRow("Type:", typeCombo);

    QCheckBox *uniqueCheck = new QCheckBox("Unique", &dialog);
    form.addRow(uniqueCheck);

    QDialogButtonBox buttons(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    connect(&buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(&buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    form.addRow(&buttons);

    if (dialog.exec() == QDialog::Accepted) {
        std::string fieldName = nameEdit->text().trimmed().toStdString();
        if (fieldName.empty()) {
            QMessageBox::warning(this, "Error", "Field name cannot be empty!");
            return;
        }
        std::string typeStr = typeCombo->currentText().toStdString();
        std::string uniqueStr = uniqueCheck->isChecked() ? " UNIQUE" : "";

        std::string sql = "ALTER TABLE " + currentTableName + " ADD " + fieldName + " " + typeStr + uniqueStr + ";";

        try {
            Interpreter interpreter;
            std::string result = interpreter.executeQuery(sql);
            if (!result.empty()) {
                QString qresult = QString::fromStdString(result);
                qresult.replace(">>> ", "");
                outputArea->append("Add field: " + qresult);
            }
        } catch (...) {
            outputArea->append("Error: Failed to add field!");
        }
        loadFieldInfo(currentTableName);
        loadTableData(currentTableName);
        refreshTree();
    }
}

// ===== 删除字段 =====
// 在字段管理表格中选中一行，获取字段名
// 生成 ALTER TABLE ... DROP COLUMN ...语句执行
void MainWindow::dropField()
{
    if (currentTableName.empty()) {
        QMessageBox::warning(this, "Warning", "Please select a table first!");
        return;
    }

    int row = fieldTableWidget->currentRow();
    if (row < 0) {
        QMessageBox::warning(this, "Warning", "Please select a field to drop!");
        return;
    }

    QString fieldName = fieldTableWidget->item(row, 0)->text();

    if (QMessageBox::question(this, "Confirm Drop",
        "Drop field '" + fieldName + "'?") != QMessageBox::Yes)
        return;

    std::string sql = "ALTER TABLE " + currentTableName + " DROP COLUMN " + fieldName.toStdString() + ";";

    try {
        Interpreter interpreter;
        std::string result = interpreter.executeQuery(sql);
        if (!result.empty()) {
            QString qresult = QString::fromStdString(result);
            qresult.replace(">>> ", "");
            outputArea->append("Drop field: " + qresult);
        }
    } catch (...) {
        outputArea->append("Error: Failed to drop field!");
    }
    loadFieldInfo(currentTableName);
    loadTableData(currentTableName);
    refreshTree();
}

// ===== 修改字段 =====
// 在字段管理表格中选中一行，弹出对话框修改类型和UNIQUE约束
// 生成 ALTER TABLE ... MODIFY COLUMN ...语句执行
void MainWindow::modifyField()
{
    if (currentTableName.empty()) {
        QMessageBox::warning(this, "Warning", "Please select a table first!");
        return;
    }

    int row = fieldTableWidget->currentRow();
    if (row < 0) {
        QMessageBox::warning(this, "Warning", "Please select a field to modify!");
        return;
    }

    QString fieldName = fieldTableWidget->item(row, 0)->text();

    QDialog dialog(this);
    dialog.setWindowTitle("Modify Field '" + fieldName + "'");
    QFormLayout form(&dialog);

    QComboBox *typeCombo = new QComboBox(&dialog);
    typeCombo->addItems(QStringList() << "INT" << "FLOAT" << "CHAR(10)" << "CHAR(20)" << "CHAR(50)");
    form.addRow("New Type:", typeCombo);

    QCheckBox *uniqueCheck = new QCheckBox("Unique", &dialog);
    form.addRow(uniqueCheck);

    QDialogButtonBox buttons(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    connect(&buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(&buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    form.addRow(&buttons);

    if (dialog.exec() == QDialog::Accepted) {
        std::string typeStr = typeCombo->currentText().toStdString();
        std::string uniqueStr = uniqueCheck->isChecked() ? " UNIQUE" : "";

        std::string sql = "ALTER TABLE " + currentTableName + " MODIFY COLUMN " + fieldName.toStdString() + " " + typeStr + uniqueStr + ";";

        try {
            Interpreter interpreter;
            std::string result = interpreter.executeQuery(sql);
            if (!result.empty()) {
                QString qresult = QString::fromStdString(result);
                qresult.replace(">>> ", "");
                outputArea->append("Modify field: " + qresult);
            }
        } catch (...) {
            outputArea->append("Error: Failed to modify field!");
        }
        loadFieldInfo(currentTableName);
        loadTableData(currentTableName);
        refreshTree();
    }
}

// ===== 执行SQL命令 =====
// 从输入框读取文本，按分号分割为多条语句，逐条执行
// 执行完毕后刷新字段信息和数据表格
void MainWindow::executeQuery()
{
    QString cmd = inputLine->toPlainText().trimmed();
    if (cmd.isEmpty()) return;

    outputArea->append(">>> " + cmd);

    // 按分号分割，支持一次粘贴多条SQL
    QStringList statements = cmd.split(';', Qt::SkipEmptyParts);
    for (const QString &stmt : statements) {
        QString trimmed = stmt.trimmed();
        if (trimmed.isEmpty()) continue;

        try {
            Interpreter interpreter;
            std::string result = interpreter.executeQuery((trimmed + ";").toStdString());
            if (!result.empty()) {
                QString qresult = QString::fromStdString(result);
                qresult.replace(">>> ", "");
                outputArea->append(qresult);
            }
        } catch (...) {
            outputArea->append("Error: Internal error!");
        }
    }

    inputLine->clear();

    // 如果当前选中了表，刷新字段和数据
    if (!currentTableName.empty()) {
        CatalogManager cm;
        if (cm.hasTable(currentTableName)) {
            loadFieldInfo(currentTableName);
            loadTableData(currentTableName);
        }
    }
    refreshTree();
    statusBar()->showMessage("Command executed: " + cmd.left(50));
}

// ===== 打开SQL脚本文件 =====
// 读取.sql文件内容，按分号分割后逐条执行
void MainWindow::openScriptFile()
{
    QString fileName = QFileDialog::getOpenFileName(this, "Open SQL Script", "", "SQL Files (*.sql);;All Files (*)");
    if (fileName.isEmpty()) return;
    QFile file(fileName);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        QMessageBox::warning(this, "Error", "Cannot open file: " + fileName);
        return;
    }
    QTextStream in(&file);
    QString content = in.readAll();
    file.close();
    outputArea->append(">>> Executing script: " + fileName);

    QStringList statements = content.split(';', Qt::SkipEmptyParts);
    for (const QString &stmt : statements) {
        QString trimmed = stmt.trimmed();
        if (!trimmed.isEmpty()) {
            try {
                Interpreter interpreter;
                std::string result = interpreter.executeQuery((trimmed + ";").toStdString());
                if (!result.empty()) {
                    QString qresult = QString::fromStdString(result);
                    qresult.replace(">>> ", "");
                    outputArea->append(qresult);
                }
            } catch (...) {
                outputArea->append("Error: Internal error!");
            }
        }
    }
    refreshTree();
}

// ===== 刷新数据库树 =====
// 扫描./database/目录下的所有子目录（每个子目录代表一个数据库）
// 读取catalog_file解析表名，再读取属性信息显示字段
// 当前使用的数据库用粗体标识
void MainWindow::refreshTree()
{
    dbTree->clear();
    QDir dbDir("./database");
    if (!dbDir.exists()) return;
    QStringList dirs = dbDir.entryList(QDir::Dirs | QDir::NoDotAndDotDot);
    for (const QString &dbName : dirs) {
        QTreeWidgetItem *dbItem = new QTreeWidgetItem(dbTree, QStringList() << dbName);
        dbItem->setIcon(0, style()->standardIcon(QStyle::SP_DirIcon));

        // 当前使用的数据库用粗体标识
        if (current_database == dbName.toStdString()) {
            QFont font = dbItem->font(0);
            font.setBold(true);
            dbItem->setFont(0, font);
        }

        // 读取catalog文件解析表名
        QDir catalogDir("./database/" + dbName + "/catalog");
        if (catalogDir.exists()) {
            QFile catalogFile("./database/" + dbName + "/catalog/catalog_file");
            if (catalogFile.open(QIODevice::ReadOnly)) {
                QByteArray data = catalogFile.readAll();
                catalogFile.close();
                QString catalogContent = QString::fromUtf8(data);
                // catalog文件格式：以#分隔的表定义块
                QStringList parts = catalogContent.split('#', Qt::SkipEmptyParts);
                for (const QString &part : parts) {
                    QString trimmed = part.trimmed();
                    if (trimmed.length() > 5) {
                        // 每个块的第5个字符后到第一个空格之间是表名
                        int start = 5;
                        int end = trimmed.indexOf(' ', start);
                        if (end > start) {
                            QString tableName = trimmed.mid(start, end - start);
                            QTreeWidgetItem *tableItem = new QTreeWidgetItem(dbItem, QStringList() << tableName);
                            tableItem->setIcon(0, style()->standardIcon(QStyle::SP_FileIcon));

                            // 临时切换数据库上下文来读取属性信息
                            CatalogManager cm;
                            std::string db_save = current_database;
                            current_database = dbName.toStdString();
                            try {
                                Attribute attr = cm.getAttribute(tableName.toStdString());
                                for (int i = 0; i < attr.num; i++) {
                                    QString typeStr;
                                    switch (attr.type[i]) {
                                        case -1: typeStr = "INT"; break;
                                        case 0: typeStr = "FLOAT"; break;
                                        default: typeStr = QString("CHAR(%1)").arg(attr.type[i] - 1); break;
                                    }
                                    QString attrInfo = QString::fromStdString(attr.name[i]) + " (" + typeStr + ")";
                                    QTreeWidgetItem *attrItem = new QTreeWidgetItem(tableItem, QStringList() << attrInfo);
                                    attrItem->setIcon(0, style()->standardIcon(QStyle::SP_FileIcon));
                                }
                            } catch (...) {}
                            current_database = db_save;
                        }
                    }
                }
            }
        }
        dbItem->setExpanded(true);
    }
}

// ===== 关于对话框 =====
void MainWindow::showAbout()
{
    QMessageBox::about(this, "About MiniSQL",
        "MiniSQL Database Management System\n\n"
        "A lightweight relational database system supporting:\n"
        "- DDL: CREATE/DROP DATABASE/TABLE/INDEX, ALTER TABLE\n"
        "- DML: INSERT, DELETE, UPDATE\n"
        "- DQL: SELECT with WHERE conditions (AND/OR)\n"
        "- Aggregate functions: COUNT, SUM, AVG, MIN, MAX\n"
        "- Views: CREATE VIEW ... AS SELECT ...\n"
        "- SQL script execution: EXECFILE / Open SQL Script\n"
        "- B+ Tree indexing\n"
        "- Database logging\n"
        "- GUI: Field management, Table data editing\n\n"
        "Built with C++ and Qt 6");
}
