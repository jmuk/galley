package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

load("@org_pubref_rules_protobuf//go:rules.bzl", "go_proto_compile")
load("@org_pubref_rules_protobuf//grpc_gateway:rules.bzl", "grpc_gateway_proto_compile")

filegroup(
    name = "galley_core_protos",
    srcs = [
        "galley/v1/config_object.proto",
        "galley/v1/error_types.proto",
        "galley/v1/metadata.proto",
    ],
)

filegroup(
    name = "service_proto",
    srcs = ["galley/v1/service.proto"],
)

go_proto_compile(
    name = "galley/v1_pb",
    importmap = {
        "google/protobuf/struct.proto": "github.com/golang/protobuf/ptypes/struct",
        "google/protobuf/empty.proto": "github.com/golang/protobuf/ptypes/empty",
        "google/api/annotations.proto": "google.golang.org/genproto/googleapis/api/annotations",
        "google/rpc/status.proto": "google.golang.org/genproto/googleapis/rpc/status",
    },
    imports = [
        "external/com_github_google_protobuf/src",
        "external/com_github_googleapis_googleapis",
    ],
    inputs = [
        "@com_github_google_protobuf//:well_known_protos",
        "@com_github_googleapis_googleapis//:annotations_proto",
        "@com_github_googleapis_googleapis//:status_proto",
    ],
    protos = [
        ":galley_core_protos",
        "galley/v1/service.proto",
    ],
    verbose = 0,
    visibility = ["//visibility:public"],
    with_grpc = True,
)

grpc_gateway_proto_compile(
    name = "galley/v1_gw",
    imports = [
        "external/com_github_google_protobuf/src",
        "external/com_github_googleapis_googleapis",
    ],
    inputs = [
        ":galley_core_protos",
        "@com_github_google_protobuf//:well_known_protos",
        "@com_github_googleapis_googleapis//:annotations_proto",
        "@com_github_googleapis_googleapis//:status_proto",
    ],
    protos = [
        "galley/v1/service.proto",
    ],
    verbose = 0,
    visibility = ["//visibility:public"],
)

go_library(
    name = "galley/v1",
    srcs = ["galley/v1_pb", "galley/v1_gw"],
    deps = [
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes/empty:go_default_library",
        "@com_github_golang_protobuf//ptypes/struct:go_default_library",
        "@org_golang_google_genproto//googleapis/api/annotations:go_default_library",
        "@org_golang_google_genproto//googleapis/rpc/status:go_default_library",
        "@com_github_grpc_ecosystem_grpc_gateway//runtime:go_default_library",
        "@com_github_grpc_ecosystem_grpc_gateway//utilities:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//grpclog:go_default_library",
    ],
)
