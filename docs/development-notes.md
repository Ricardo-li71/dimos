# Development Notes

## 背景

本项目是在开源 Dimos 框架基础上进行的个人应用开发，重点不是重写机器人底层框架，而是完成 AI Agent 到真实 Unitree Go2 机器人的部署、接入和真机验证。

## 关键问题

### 1. 跨网络通信

开发机、机器人侧设备和运动控制板不总在同一可直接访问的网络中。为了解决连接问题，项目中使用 SSH 隧道和 Jetson 端口转发脚本打通 WebRTC 控制链路。

### 2. Agent 模型接入

将原有本地模型配置调整为 Qwen3-VL-Plus，使 Agent 能够解析自然语言指令并调度机器人技能。

### 3. 真机控制验证

在 Unitree Go2 上验证了基础运动命令闭环，包括前进、转弯和指定距离 / 角度控制。

## 上游依赖

- Dimos: https://github.com/dimensionalOS/dimos
- Unitree Go2 WebRTC control stack
- Qwen3-VL-Plus API

## 个人贡献总结

- 环境部署与依赖调试
- 跨网络通信方案设计
- Qwen3-VL-Plus Agent 接入
- 真机联调与基础运动指令验证
- 辅助脚本和部署文档整理
