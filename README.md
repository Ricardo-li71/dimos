# Dimos

基于开源 Dimos 框架的 Unitree Go2 四足机器人自然语言控制系统。项目目标是把大模型 Agent 接入真实机器人控制链路，让用户通过自然语言指令完成基础运动控制，例如“前进 1 米”“左转 90 度”。

> 说明：本仓库是我的个人项目展示仓库，记录我在开源 Dimos 框架基础上的部署、集成与应用开发工作。底层机器人框架来自 [dimensionalOS/dimos](https://github.com/dimensionalOS/dimos)。

## 项目概览

- 面向场景：四足机器人自然语言交互与 AI Agent 控制
- 机器人平台：Unitree Go2
- 大模型能力：Qwen3-VL-Plus，多模态指令解析与技能调度
- 控制链路：自然语言指令 -> LLM Agent -> 技能调用 -> A* 路径规划 -> 机器人运动执行
- 当前进展：已在 Unitree Go2 真机上跑通基础运动指令识别与执行

## 我的工作

- 独立完成 Dimos 框架在本地开发机与机器人环境中的部署和调试
- 设计跨网络通信方案，通过 SSH 隧道与 WebRTC 打通开发机、Jetson / 机器人主板和运动控制链路
- 接入 Qwen3-VL-Plus 作为 Agent 决策引擎，支持中英文自然语言指令解析
- 跑通 Unitree Go2 真机控制闭环，支持前进、转弯及指定距离 / 角度控制
- 编写 Jetson 端端口转发脚本，解决特定网络环境下的 WebRTC 端口访问问题

## 技术栈

- Python
- Dimos
- Unitree Go2
- WebRTC
- SSH Tunnel
- LangChain / LLM Agent
- Qwen3-VL-Plus
- A* Path Planning
- FastAPI

## 仓库结构

```text
.
├── README.md
├── docs
│   ├── architecture.md
│   └── development-notes.md
└── scripts
    └── jetson_forwarder.py
```

## 运行说明

本仓库不包含完整 Dimos 源码。复现时需要先参考上游项目完成 Dimos 环境安装，再根据 `docs/development-notes.md` 配置机器人网络、模型服务和端口转发脚本。

```bash
python scripts/jetson_forwarder.py
```

## 后续计划

- 补充前端控制界面或 Demo 页面
- 增加演示视频 / GIF，展示自然语言到真机运动的完整流程
- 将基础运动指令扩展为更稳定的任务级控制，例如“绕过障碍后到达目标点”
- 整理更完整的部署文档，降低复现成本
