load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "common.go",
        "completion.go",
        "create.go",
        "delete.go",
        "get.go",
        "replace.go",
        "root.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//api:galley/v1",  # keep
        "//cmd/shared:go_default_library",
        "//pkg/client:go_default_library",
        "//pkg/client/config:go_default_library",
        "//pkg/client/file:go_default_library",
        "@com_github_spf13_cobra//:go_default_library",
    ],
)
