#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "datasystem/transfer_engine/status.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace py = pybind11;

namespace datasystem {
namespace {

class PyTransferEngine {
public:
    PyTransferEngine()
    {
        engine_ = std::make_unique<TransferEngine>();
    }

    Result InitializeLegacy(const std::string &localHostname, const std::string &protocol,
                            const std::string &deviceName)
    {
        Result result = engine_->Initialize(localHostname, protocol, deviceName);
        if (result.IsOk()) {
            std::lock_guard<std::mutex> lock(deviceNameMutex_);
            deviceName_ = deviceName;
        }
        return result;
    }

    Result Initialize(const std::string &localHostname, const std::string &metadataServer, const std::string &protocol,
                      const std::string &deviceName)
    {
        Result result = engine_->Initialize(localHostname, metadataServer, protocol, deviceName);
        if (result.IsOk()) {
            std::lock_guard<std::mutex> lock(deviceNameMutex_);
            deviceName_ = deviceName;
        }
        return result;
    }

    Result RegisterMemory(uintptr_t bufferAddr, size_t capacity, const std::string &location)
    {
        Result locationRc = ValidateLocation(location);
        return locationRc.IsError() ? locationRc : engine_->RegisterMemory(bufferAddr, capacity);
    }

    int32_t GetRpcPort()
    {
        return engine_->GetRpcPort();
    }

    std::string GetRoutePolicy()
    {
        return engine_->GetRoutePolicy();
    }

    Result BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddresses, const std::vector<size_t> &capacities,
                               const std::string &location)
    {
        Result locationRc = ValidateLocation(location);
        return locationRc.IsError() ? locationRc : engine_->BatchRegisterMemory(bufferAddresses, capacities);
    }

    Result RegisterMemoryEx(const MemoryRegistration &registration, const std::string &location)
    {
        Result locationRc = ValidateLocation(location);
        return locationRc.IsError() ? locationRc : engine_->RegisterMemoryEx(registration);
    }

    Result BatchRegisterMemoryEx(const std::vector<MemoryRegistration> &registrations, const std::string &location)
    {
        Result locationRc = ValidateLocation(location);
        return locationRc.IsError() ? locationRc : engine_->BatchRegisterMemoryEx(registrations);
    }

    Result UnregisterMemory(uintptr_t bufferAddr)
    {
        return engine_->UnregisterMemory(bufferAddr);
    }

    Result BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs)
    {
        return engine_->BatchUnregisterMemory(bufferAddrs);
    }

    Result TransferSyncRead(const std::string &targetHostname, uintptr_t buffer, uintptr_t peerBufferAddress,
                            size_t length, const std::string &transportHint)
    {
        Result hintRc = ValidateTransportHint(transportHint);
        return hintRc.IsError() ? hintRc : engine_->TransferSyncRead(targetHostname, buffer, peerBufferAddress, length);
    }

    Result BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                 const std::vector<uintptr_t> &peerBufferAddresses, const std::vector<size_t> &lengths,
                                 const std::string &transportHint)
    {
        Result hintRc = ValidateTransportHint(transportHint);
        return hintRc.IsError() ? hintRc
                                : engine_->BatchTransferSyncRead(targetHostname, buffers, peerBufferAddresses, lengths);
    }

    Result Finalize()
    {
        return engine_->Finalize();
    }

private:
    Result ValidateLocation(const std::string &location) const
    {
        std::lock_guard<std::mutex> lock(deviceNameMutex_);
        if (location.empty() || location == "*" || location == deviceName_) {
            return Result::OK();
        }
        return Result(ErrorCode::kNotSupported,
                      "location should be empty, '*', or the initialized NPU device for YuanRong TransferEngine");
    }

    static Result ValidateTransportHint(const std::string &transportHint)
    {
        if (transportHint.empty() || transportHint == "ascend") {
            return Result::OK();
        }
        return Result(ErrorCode::kNotSupported,
                      "transport_hint does not select HCCS or RoCE; configure the YuanRong HIXL route policy");
    }

    std::unique_ptr<TransferEngine> engine_;
    mutable std::mutex deviceNameMutex_;
    std::string deviceName_;
};

void BindErrorCode(py::module_ &m)
{
    py::enum_<datasystem::ErrorCode>(m, "ErrorCode")
        .value("kOk", datasystem::ErrorCode::kOk)
        .value("kInvalid", datasystem::ErrorCode::kInvalid)
        .value("kNotFound", datasystem::ErrorCode::kNotFound)
        .value("kRuntimeError", datasystem::ErrorCode::kRuntimeError)
        .value("kNotReady", datasystem::ErrorCode::kNotReady)
        .value("kNotAuthorized", datasystem::ErrorCode::kNotAuthorized)
        .value("kNotSupported", datasystem::ErrorCode::kNotSupported)
        .export_values();
}

void BindResult(py::module_ &m)
{
    py::class_<datasystem::Result>(m, "Result")
        .def(py::init<>())
        .def("is_ok", &datasystem::Result::IsOk)
        .def("is_error", &datasystem::Result::IsError)
        .def("get_code", &datasystem::Result::GetCode)
        .def("get_msg", &datasystem::Result::GetMsg)
        .def("to_string", &datasystem::Result::ToString)
        .def("__repr__", [](const datasystem::Result &s) { return std::string("Result(") + s.ToString() + ")"; });
}

void BindMemoryRegistration(py::module_ &m)
{
    py::class_<datasystem::MemoryRegistration>(m, "MemoryRegistration")
        .def(py::init<>())
        .def(py::init<uintptr_t, size_t, uintptr_t, size_t>(), py::arg("logical_addr"), py::arg("logical_length"),
             py::arg("backing_addr"), py::arg("backing_length"))
        .def_readwrite("logical_addr", &datasystem::MemoryRegistration::logicalAddr)
        .def_readwrite("logical_length", &datasystem::MemoryRegistration::logicalLength)
        .def_readwrite("backing_addr", &datasystem::MemoryRegistration::backingAddr)
        .def_readwrite("backing_length", &datasystem::MemoryRegistration::backingLength);
}

void BindTransferEngine(py::module_ &m)
{
    py::class_<datasystem::PyTransferEngine>(m, "TransferEngine")
        .def(py::init<>())
        .def("initialize", &datasystem::PyTransferEngine::Initialize, py::arg("local_hostname"),
             py::arg("metadata_server"), py::arg("protocol"), py::arg("device_name"),
             py::call_guard<py::gil_scoped_release>())
        .def("initialize", &datasystem::PyTransferEngine::InitializeLegacy, py::arg("local_hostname"),
             py::arg("protocol"), py::arg("device_name"), py::call_guard<py::gil_scoped_release>())
        .def("get_rpc_port", &datasystem::PyTransferEngine::GetRpcPort)
        .def("get_route_policy", &datasystem::PyTransferEngine::GetRoutePolicy)
        .def("register_memory", &datasystem::PyTransferEngine::RegisterMemory, py::arg("buffer_addr"),
             py::arg("capacity"), py::arg("location") = "*", py::call_guard<py::gil_scoped_release>())
        .def("batch_register_memory", &datasystem::PyTransferEngine::BatchRegisterMemory, py::arg("buffer_addresses"),
             py::arg("capacities"), py::arg("location") = "*", py::call_guard<py::gil_scoped_release>())
        .def("register_memory_ex", &datasystem::PyTransferEngine::RegisterMemoryEx, py::arg("registration"),
             py::arg("location") = "*", py::call_guard<py::gil_scoped_release>())
        .def("batch_register_memory_ex", &datasystem::PyTransferEngine::BatchRegisterMemoryEx, py::arg("registrations"),
             py::arg("location") = "*", py::call_guard<py::gil_scoped_release>())
        .def("unregister_memory", &datasystem::PyTransferEngine::UnregisterMemory, py::arg("buffer_addr"),
             py::call_guard<py::gil_scoped_release>())
        .def("batch_unregister_memory", &datasystem::PyTransferEngine::BatchUnregisterMemory,
             py::arg("buffer_addresses"), py::call_guard<py::gil_scoped_release>())
        .def("transfer_sync_read", &datasystem::PyTransferEngine::TransferSyncRead, py::arg("target_hostname"),
             py::arg("buffer"), py::arg("peer_buffer_address"), py::arg("length"), py::arg("transport_hint") = "",
             py::call_guard<py::gil_scoped_release>())
        .def("batch_transfer_sync_read", &datasystem::PyTransferEngine::BatchTransferSyncRead,
             py::arg("target_hostname"), py::arg("buffers"), py::arg("peer_buffer_addresses"), py::arg("lengths"),
             py::arg("transport_hint") = "", py::call_guard<py::gil_scoped_release>())
        .def("finalize", &datasystem::PyTransferEngine::Finalize, py::call_guard<py::gil_scoped_release>());
}

}  // namespace

void BindTransferEngineModule(py::module_ &m)
{
    BindErrorCode(m);
    BindResult(m);
    BindMemoryRegistration(m);
    BindTransferEngine(m);
}

}  // namespace datasystem

PYBIND11_MODULE(_transfer_engine, m)
{
    m.doc() = "Python bindings for transfer_engine";
    datasystem::BindTransferEngineModule(m);
}
