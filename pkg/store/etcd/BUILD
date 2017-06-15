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
        "@com_github_coreos_etcd//clientv3/concurrency:go_default_library",
        "@com_github_coreos_etcd//mvcc/mvccpb:go_default_library",
        "@com_github_golang_glog//:go_default_library",
    ],
)

go_test(
    name = "small_tests",
    size = "small",
    srcs = [
        "etcdstore_test.go",
    ],
    library = ":go_default_library",
    deps = [
        ":go_default_library",
        "//pkg/store/testutil:go_default_library",
    ],
)
