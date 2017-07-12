load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "completion.go",
        "create.go",
        "delete.go",
        "get.go",
        "replace.go",
        "root.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//cmd/shared:go_default_library",
        "@com_github_spf13_cobra//:go_default_library",
    ],
)
