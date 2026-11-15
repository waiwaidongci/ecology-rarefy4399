# gb-0014 测试记录

## 环境

本机无 Docker/Postgres/Redis，因此本批次验证用 SQLite + Celery eager 模式，真实复用逻辑、租约语义与 API 路由全部执行；未做容器内 Postgres 并发压测（见“已知差异”）。

- Python 3.11.2，fastapi 0.115.6 / SQLAlchemy 2.0.36 / numpy 2.2.1 / scipy 1.15.0 / celery 5.4.0
- 前端：TypeScript 5.7 `tsc --noEmit` 与 `npm run build` 通过
- 测试文件：`tests/test_shared_computations.py`、`tests/test_api_shared_runs.py`
- 日志：`logs/pytest.log`

## 结果

```
python3 -m pytest test-artifacts/gb-0014/tests -q
15 passed, 1 warning in 6.99s
```

| 验证点 | 测试 |
|---|---|
| 重叠选线复用，未算过的样线才入队 | `test_overlap_selection_reuses_and_matches_independent`、`test_full_overlap_reuses_without_new_computation` |
| 复用结果与独立计算逐项相同（仅多来源标注）；指纹不变 | `test_overlap_selection_reuses_and_matches_independent` |
| m/seed 不同不共享 | `test_different_params_do_not_share` |
| 并发领取只保留一个有效执行者（两个独立会话） | `test_concurrent_claim_only_one_executor` |
| 失联后接管并重算；旧执行者晚到结果不写入 | `test_takeover_after_lost_worker_and_late_result_dropped`、`test_stale_executor_dropped_while_new_owner_running` |
| 停止一项不影响仍依赖该计算的研究 | `test_stop_one_run_does_not_break_shared_dependent`、`test_shared_then_stopped_during_computation` |
| 停止后不发布结果，worker 竞态不复活 | `test_worker_assembly_does_not_revive_stopped_run`、`test_stop_releases_exclusive_unfinished_computation` |
| API：复用来源可见、停止路由、viewer 403、已完成 409 | `tests/test_api_shared_runs.py` |
| 失败计算只报错不循环重算 | `test_failed_computation_reported_not_looped` |

另外：`python3 -m scripts.seed_demo` 在 SQLite 上生成 3 调查期 / 54 样线期次 / 1296 计数 / 1 基准研究；基准研究与独立 `analyze()` 对比，去掉 `source` 标注后逐项相同，指纹相同；共享表 17 行（18 条选中样线中 1 条为全零空线，不建共享行）。

指纹兼容性：新旧 `fingerprint()` 对同一输入输出相同 SHA-256（已用 HEAD 版本对比验证）。

## 已知差异

- 未在本机启动 Docker 栈；Postgres 下的并发领取依赖同一条 `UPDATE ... WHERE status/lease` 的条件更新语义，与 SQLite 测试同源，但没有真实多 worker 压测。
- worker 的接管扫描由内嵌 Celery beat 每30秒触发（`--beat`），本机未运行真实 Redis broker，接管用 `reap_expired.run()` 直接调用验证。
- 未生成浏览器截图（无可用浏览器/容器）。
