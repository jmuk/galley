package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "inventory.go",
    ],
    deps = [
        "//pkg/store:go_default_library",
        "//pkg/store/redis:go_default_library",
    ],
)
