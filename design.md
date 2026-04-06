## Plan: Airflow DataX OpenLineage Operator

创建 Python 包 `airflow-provider-datax-openlineage`，提供自定义 Airflow Operator，通过命令行执行 DataX 数据同步任务，同时解析 DataX job 配置提取血缘信息（reader/writer → InputDataset/OutputDataset），通过 OpenLineage Python Client 发送 START/COMPLETE/FAIL 事件到指定 API 地址。使用 uv 管理依赖，Python 3.10+。

---

### 项目结构

```
airflow_datax_openlineage/
├── pyproject.toml
├── README.md
├── src/
│   └── airflow_provider_datax_openlineage/
│       ├── __init__.py
│       ├── operators/
│       │   ├── __init__.py
│       │   └── datax.py                    # DataXOperator 核心
│       └── lineage/
│           ├── __init__.py
│           ├── parser.py                   # DataX config → lineage 解析调度
│           └── plugins/
│               ├── __init__.py
│               ├── base.py                 # BaseDataXLineagePlugin 抽象基类
│               ├── mysql.py                # MySQL reader/writer
│               └── postgres.py             # Postgres reader/writer
└── tests/
    ├── conftest.py
    ├── test_datax_operator.py
    ├── test_lineage_parser.py
    └── test_plugins/
        ├── test_mysql.py
        └── test_postgres.py
```

---

### Steps

#### Phase 1: 项目脚手架 (steps 1-2)

1. **初始化 uv 项目** — 创建 `pyproject.toml`（hatchling build backend, src layout），依赖: `apache-airflow>=2.7.0`, `openlineage-python>=1.0.0`; dev 依赖: `pytest`, `pytest-mock`
2. **创建目录结构** — 所有目录和 `__init__.py` 文件

#### Phase 2: 血缘解析框架 (steps 3-6)

3. **`BaseDataXLineagePlugin`** — 抽象基类，定义 `reader_names()`, `writer_names()`, `parse_dataset(plugin_name, plugin_config) -> Dataset` 方法
4. **MySQL 插件** — 解析 `mysqlreader/mysqlwriter` 的 `parameter.connection[].jdbcUrl` 提取 host/port/database，`parameter.connection[].table` 提取表名，构建 namespace `mysql://host:port`，name `database.table`
5. **Postgres 插件** — *parallel with step 4*，类似 MySQL，解析 `postgresqlreader/postgresqlwriter`，namespace `postgres://host:port`
6. **LineageParser** — 自动注册插件，遍历 `job.content[]`，根据 reader/writer name 匹配插件解析，未匹配时 log warning 不中断

#### Phase 3: DataX Operator (steps 7-8, depends on Phase 2)

7. **DataXOperator** — 继承 `BaseOperator`，参数:
   - `job_config: str` (template_field) — DataX job JSON 路径
   - `datax_home: str | None` — 默认 fallback 环境变量 `DATAX_HOME`
   - `datax_python: str = "python"` — 执行 datax.py 的 Python
   - `jvm_params: dict | None` (template_field)
   - `openlineage_url: str | None` — 默认 fallback 环境变量 `OPENLINEAGE_URL`
   - `openlineage_namespace: str = "datax"`
   - **execute 流程**: 读取 JSON → LineageParser 提取 datasets → 发送 START → subprocess 执行 DataX → 成功发送 COMPLETE / 失败发送 FAIL + raise AirflowException
   - `on_kill()` terminate subprocess

8. **OpenLineage 事件构建** — 辅助方法 `_build_run_event()`，producer/job name 从 Airflow context 获取 (`{dag_id}.{task_id}`)

#### Phase 4: 测试 (steps 9-11, depends on Phase 2 & 3)

9. **Lineage 插件测试** — MySQL/Postgres config fixture，验证 namespace/name 解析、多表场景、JDBC URL 容错
10. **LineageParser 测试** — 完整解析流程，未知 reader/writer 时仅 warning
11. **DataXOperator 测试** — mock subprocess (成功/失败)，mock `OpenLineageClient.emit` 验证 START/COMPLETE/FAIL 事件，验证命令行构建、环境变量 fallback

#### Phase 5: 文档 (step 12)

12. **README.md** — 安装、配置、DAG 使用示例、插件扩展指南

---

### Verification

1. `uv sync` — 依赖安装成功
2. `uv run pytest tests/ -v` — 全部测试通过
3. `uv run python -c "from airflow_provider_datax_openlineage.operators.datax import DataXOperator"` — import 正常
4. 手动验证: 构造 MySQL→Postgres DataX job JSON，检查命令构建和 OpenLineage 事件发送

---

### Decisions

- Job config 由用户提前准备，operator 只接收路径
- 直接使用 openlineage-python client 发送事件，不走 Airflow Provider 集成
- 初期支持 MySQL + Postgres，提供可扩展插件框架
- DataX home 和 OpenLineage URL 均支持 operator 参数 + 环境变量 fallback
- 纯单元测试，mock subprocess 和 OpenLineage client

---

### Further Considerations

1. **DataX 执行统计**: DataX stdout 包含读取/写入记录数统计，V1 先不解析，未来可作为 OpenLineage facet 发送
2. **Airflow Connection 集成**: 当前 DataX job config 中连接信息写死，V2 可考虑从 Airflow Connection 注入凭证
3. **多 content 支持**: DataX `content` 是数组，当前设计遍历所有条目，每个 reader/writer 各自生成 dataset