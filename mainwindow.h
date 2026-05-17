// mainwindow.h - MiniSQL GUI主窗口头文件
// 定义了主窗口类MainWindow，包含所有UI控件和事件处理函数的声明。
//
// 界面布局：
// ┌──────────────┬──────────────────────────────────┐
// │              │  输出区（QTextEdit，只读）         │
// │  数据库树     │                                  │
// │  (QTreeWidget)│──────────────────────────────────│
// │  显示库/表/字段│  输入区（QPlainTextEdit，多行）    │
// │              │  [Execute] 按钮                   │
// │              │──────────────────────────────────│
// │              │  字段管理区（QTableWidget + 按钮）  │
// │              │──────────────────────────────────│
// │              │  数据表格区（QTableWidget + 按钮）  │
// └──────────────┴──────────────────────────────────┘

#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QTextEdit>
#include <QPlainTextEdit>
#include <QTreeWidget>
#include <QTableWidget>
#include <QSplitter>
#include <QStatusBar>
#include <QMenuBar>
#include <QMenu>
#include <QAction>
#include <QMessageBox>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QFileDialog>
#include <QTextStream>
#include <QString>
#include <QPushButton>
#include <QHeaderView>
#include <QInputDialog>
#include <QComboBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QCheckBox>
#include <QDialog>
#include <QDialogButtonBox>

class MainWindow : public QMainWindow {
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

private slots:
    // 执行SQL命令：从输入框读取SQL，按分号分割后逐条执行
    void executeQuery();
    // 打开SQL脚本文件并批量执行其中的语句
    void openScriptFile();
    // 刷新左侧数据库树形结构
    void refreshTree();
    // 显示关于对话框
    void showAbout();
    // 点击树形结构中的项目时触发：加载对应表的字段信息和数据
    void onTreeItemClicked(QTreeWidgetItem *item, int column);
    // 数据表格单元格被编辑时触发：自动生成UPDATE语句更新数据库
    void onTableDataChanged(int row, int col);
    // 弹出对话框输入各字段值，生成INSERT语句插入新行
    void addRow();
    // 删除数据表格中当前选中的行，生成DELETE语句
    void deleteRow();
    // 弹出对话框输入新字段信息，生成ALTER TABLE ADD语句
    void addField();
    // 删除字段管理表格中选中的字段，生成ALTER TABLE DROP COLUMN语句
    void dropField();
    // 弹出对话框修改选中字段的类型和约束，生成ALTER TABLE MODIFY COLUMN语句
    void modifyField();

private:
    // 初始化UI布局：左侧树 + 右侧输出/输入/字段管理/数据表格
    void setupUI();
    // 初始化菜单栏：File, View, Help
    void setupMenu();
    // 从数据库读取指定表的所有记录，显示到数据表格中
    void loadTableData(const std::string &tableName);
    // 从catalog读取指定表的字段定义，显示到字段管理表格中
    void loadFieldInfo(const std::string &tableName);

    // SQL输出区域：显示查询结果和执行信息
    QTextEdit *outputArea;
    // SQL输入区域：支持多行输入，按分号分割执行多条语句
    QPlainTextEdit *inputLine;
    // 左侧数据库树：三级结构（数据库→表→字段）
    QTreeWidget *dbTree;
    // 右下数据表格：显示选中表的所有记录，支持直接编辑单元格
    QTableWidget *dataTableWidget;
    // 字段管理表格：显示选中表的字段定义（名称/类型/唯一/主键）
    QTableWidget *fieldTableWidget;
    // 当前选中表名标签
    QLabel *currentTableLabel;
    // 当前选中的表名（空表示未选中任何表）
    std::string currentTableName;
    // 防止loadTableData时触发onTableDataChanged的标志
    bool isUpdatingTable;
};

#endif
