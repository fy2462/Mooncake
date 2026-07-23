# Transfer Engine

本节从稳定概念到 native 实现逐层深入：先理解 TE 的职责，再审视 FFI 所有权，随后
进入 RDMA transport，最后把一次 batch 的创建、提交、完成和清理串起来。

```{toctree}
:maxdepth: 1

overview
ffi-boundary
transport-and-rdma
transfer-lifecycle
```

本节解释 Rust FFI 到 C++ Transfer Engine/TENT 的边界，以及内存注册、
Segment 打开、批量请求提交、transport 选择和 RDMA completion 的生命周期。
