ImpleForge 技术规格说明书 (Technical Specification)
版本: 1.0

代号: Start-Up

日期: 2026-02-18

适用范围: Windows 环境实施、运维、数据库维护

1. 项目概述 (Overview)
ImpleForge 是一个面向实施工程师的、模块化的 Windows 桌面工具箱。
它的核心设计目标是 “配置驱动 (Config-Driven)” 和 “插件化 (Plugin-Based)”。它不硬编码具体业务逻辑，而是提供一个可扩展的容器，通过加载 JSON 配置文件和外部脚本/插件，动态生成操作界面。

1.1 核心价值
便携性 (Portable): 无需安装，解压即用 (Green/No-Install)。

扩展性 (Extensible): 通过修改 JSON 和添加 Python 脚本即可增加新功能，无需重新编译主程序。

留痕 (Traceability): 所有操作具备日志记录能力。

2. 系统架构 (Architecture)
系统采用 "Shell + Kernel + Plugins" 三层架构。

2.1 层级定义
UI Shell (界面层): 基于 PySide6 (Qt)。负责渲染主窗口、解析 JSON 生成菜单树、以及展示右侧的功能面板。

Core Kernel (内核层):

Loader: 负责递归解析 menu.json。

Executor: 统一的执行引擎，负责调度 CMD 命令、PowerShell 脚本或 Python 函数。

Logger: 全局日志系统，捕获标准输出 (stdout/stderr) 并持久化。

Plugin Layer (插件/脚本层):

Native Scripts: .bat, .ps1, .sql 文件。

Python Plugins: 继承自标准接口的 Python QWidget 类（用于复杂交互）。

3. 功能需求 (Functional Requirements)
3.1 动态菜单系统 (Dynamic Menu System)
FR-01: 系统启动时必须读取 config/menu.json。

FR-02: 支持 无限级 树状菜单结构（Category -> Sub-Category -> Action）。

FR-03: 菜单项需支持图标 (icon)、名称 (name)、描述 (desc) 配置。

3.2 执行引擎 (Execution Engine)
系统需支持三种类型的操作模式 (ActionType)：

Type A: Quick Command (CMD/Shell)

直接调用 Windows 终端执行命令（如 ping, ipconfig）。

必须 异步执行 (QThread)，防止界面卡死。

必须 实时捕获输出流，并显示在“控制台面板”中。

Type B: Script Runner (File-based)

调用外部文件（如 scripts/backup_oracle.bat）。

支持参数注入（例如从 UI 输入框获取 IP 地址传给脚本）。

Type C: UI Plugin (Complex Widget)

动态加载 Python 模块（importlib）。

在右侧内容区实例化自定义的 QWidget。

场景示例: 数据库连接配置界面、日志分析可视化图表。

3.3 日志与反馈 (Logging & Feedback)
FR-04: 底部必须包含一个全局状态栏/日志窗口。

FR-05: 关键操作（如“执行脚本成功/失败”）必须写入本地日志文件 logs/operation.log。

4. 数据结构规范 (Data Structures)
这是核心协议，所有扩展都依赖于此 JSON 结构。

4.1 菜单配置 (config/menu.json)
JSON

{
  "app_name": "ImpleForge Toolkit",
  "version": "1.0.0",
  "menu_structure": [
    {
      "id": "group_sys",
      "name": "系统维护",
      "type": "group",
      "icon": "system.png",
      "children": [
        {
          "id": "cmd_ip",
          "name": "查看IP配置",
          "type": "command", 
          "cmd": "ipconfig /all",
          "description": "列出所有网络适配器的详细信息"
        },
        {
          "id": "script_clean",
          "name": "清理临时文件",
          "type": "script",
          "path": "scripts/clean_temp.bat"
        }
      ]
    },
    {
      "id": "plugin_db",
      "name": "数据库工具",
      "type": "group",
      "children": [
        {
          "id": "widget_db_conn",
          "name": "连接测试器",
          "type": "plugin",
          "module": "plugins.database.connector",
          "class_name": "DBConnectorWidget"
        }
      ]
    }
  ]
}
5. UI/UX 规范 (Design Spec)
主窗口尺寸: 默认 1024x768，允许调整大小。

布局模式: QHBoxLayout (水平布局)。

左侧 (Left): QTreeWidget (宽 250px, 可折叠)。

右侧 (Right): QStackedWidget (自适应宽度)。

配色方案 (Theme): 深色模式 (Dark Theme)。

背景: #1E1E1E

文本: #D4D4D4

高亮: #007ACC (VS Code 蓝)

6. 技术约束 (Constraints)
运行环境: Windows 10/11, Windows Server 2012+。

解释器: Python 3.11.x。

打包: 必须使用 PyInstaller 生成单目录 (--onedir) 模式，以便用户可以直接替换 config/ 或 scripts/ 下的文件而不需要重新编译 exe。

编码: 全局强制使用 UTF-8，并在处理 Windows CMD 输出时自动处理 GBK 解码（这是 Windows 中文版常见坑）。

7. 开发路线图 (Roadmap)
Phase 1: 骨架构建 (Current Focus)
[ ] 搭建 PySide6 主窗口。

[ ] 实现 JSON 递归读取器。

[ ] 实现左侧菜单与右侧页面的点击联动。

[ ] 交付物: 一个带菜单的可运行空壳程序。

Phase 2: 基础执行器
[ ] 实现 CommandExecutor 类 (QThread)。

[ ] 解决 Python 调用 CMD 的中文乱码问题。

[ ] 实现基础的日志回显面板。

[ ] 交付物: 可以点击菜单运行 ping baidu.com 并看到结果的版本。

Phase 3: 插件系统与高级功能
[ ] 实现 PluginLoader (动态 import)。

[ ] 开发第一个插件：数据库连接配置面板 (SQLAlchemy)。

[ ] 交付物: 具备数据库操作能力的完整版本。