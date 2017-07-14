package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["inventory.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/store:go_default_library",
        "//pkg/store/etcd:go_default_library",
        "//pkg/store/memstore:go_default_library",
    ],
)
