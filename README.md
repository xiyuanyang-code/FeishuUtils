# Feishu Utils

> 基于飞书群组 API 的一些定时提效小工具。

## 功能

### 1. 记账助手 (Bookkeeping)
自动从飞书群组提取记账消息，导出为 CSV 格式。

### 2. GPU 监控 (GPU Monitor)
监控远程 GPU 节点状态，定期向飞书群组报告 GPU 使用情况。

## 使用方法

### 记账功能
```bash
python main.py --task_type book
```

### GPU 监控

**单次运行**（执行一次监控后退出）：
```bash
python main.py --task_type gpu --run_once
```

**持续运行**（检测到新消息时自动发送 GPU 状态）：
```bash
python main.py --task_type gpu --continue_run
```

## 配置

在 `config/config.yaml` 中配置飞书应用信息和 GPU 节点名称。

## Requirements

