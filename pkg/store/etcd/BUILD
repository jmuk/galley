package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "etcdstore.go",
    ],
    deps = [
        "//pkg/store:go_default_library",
        "@com_github_coreos_etcd//clientv3:go_default_library",
        "@com_github_coreos_etcd//mvcc/mvccpb:go_default_library",
        "@com_github_golang_glog//:go_default_library",
    ],
)
