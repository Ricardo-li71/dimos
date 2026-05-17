# Architecture

## 控制链路

```text
User instruction
  -> LLM Agent
  -> Skill dispatch
  -> A* path planning
  -> WebRTC command channel
  -> Unitree Go2 motion execution
```

## 网络链路

项目部署中需要连接本地开发机、机器人侧计算设备和运动控制板。实际调试中遇到跨子网访问与端口限制问题，因此采用三层通信方案：

- 本地开发机负责运行 Agent、调试代码和观察日志
- SSH 隧道负责跨网络转发关键服务端口
- WebRTC 负责与 Unitree Go2 运动控制链路通信

## Agent 设计

Agent 接收自然语言指令后，将用户意图转换为可执行技能调用。当前重点支持基础运动控制：

- 前进 / 后退
- 左转 / 右转
- 指定距离移动
- 指定角度转向

## 当前边界

- 当前主要验证基础运动指令和端到端链路
- 复杂任务规划、动态避障和长距离自主导航仍在迭代中
- 真机部署依赖具体网络环境，复现时需要根据机器人和开发机 IP 配置调整
