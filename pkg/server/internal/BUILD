load("@org_pubref_rules_protobuf//go:rules.bzl", "go_proto_library")

go_proto_library(
    name = "go_default_library",
    importmap = {
        "api/galley/v1/config_object.proto": "istio.io/galley/api/galley/v1",
        "api/galley/v1/service.proto": "istio.io/galley/api/galley/v1",
    },
    imports = [
        "external/com_github_google_protobuf/src",
        "external/com_github_googleapis_googleapis",
    ],
    inputs = [
        "//api:galley_core_protos",
        "//api:service_proto",
        "@com_github_google_protobuf//:well_known_protos",
        "@com_github_googleapis_googleapis//:annotations_proto",
        "@com_github_googleapis_googleapis//:status_proto",
    ],
    protos = [
        "file.proto",
    ],
    verbose = 0,
    visibility = ["//pkg/server:__subpackages__"],
    deps = [
        "//api:galley/v1",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)
