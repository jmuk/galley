package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "galley.go",
        "service.go",
        "util.go",
        "watcher.go",
    ],
    deps = [
        "//pkg/store:go_default_library",
        "//pkg/store/inventory:go_default_library",
        "@com_github_golang_protobuf//jsonpb:go_default_library",
        "@com_github_golang_protobuf//ptypes/empty:go_default_library",
        "@com_github_golang_protobuf//ptypes/struct:go_default_library",
        "@com_github_istio_api//:config/v1",
        "@org_golang_x_net//context:go_default_library",
    ],
)
