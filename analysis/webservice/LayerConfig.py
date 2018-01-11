"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

ALL_LAYERS_ENABLED = True
LAYERS = []
LAYERS.append({"shortName": "NCDC-L4LRblend-GLOB-AVHRR_OI", "envs": ("ALL",)})
LAYERS.append({"shortName": "SSH_alti_1deg_1mon", "envs": ("ALL",)})

LAYERS.append({"shortName": "SIacSubl_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "PHIBOT_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SIhsnow_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SIheff_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "oceFWflx_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "oceQnet_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "MXLDEPTH_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SIatmQnt_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "oceSPflx_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "oceSPDep_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SIarea_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "ETAN_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "sIceLoad_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "oceQsw_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SIsnPrcp_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "DETADT2_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "TFLUX_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SItflux_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "SFLUX_ECCO_version4_release1", "envs": ("ALL",)})
LAYERS.append({"shortName": "TELLUS_GRACE_MASCON_GRID_RL05_V1_LAND", "envs": ("ALL",)})
LAYERS.append({"shortName": "TELLUS_GRACE_MASCON_GRID_RL05_V1_OCEAN", "envs": ("ALL",)})
LAYERS.append({"shortName": "Sea_Surface_Anomalies", "envs": ("DEV",)})

LAYERS.append({"shortName": "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1", "envs": ("ALL",)})


def isLayerEnabled(shortName, env):
    if ALL_LAYERS_ENABLED:
        return True
    if env == None:
        env = "PROD"
    env = env.upper()
    if env == "DEV":
        return True
    for layer in LAYERS:
        if layer["shortName"] == shortName and ("ALL" in layer["envs"] or env.upper() in layer["envs"]):
            return True
    return False


if __name__ == "__main__":
    print isLayerEnabled("NCDC-L4LRblend-GLOB-AVHRR_OI", None)
    print isLayerEnabled("NCDC-L4LRblend-GLOB-AVHRR_OI", "PROD")
    print isLayerEnabled("NCDC-L4LRblend-GLOB-AVHRR_OI", "SIT")

    print isLayerEnabled("TFLUX_ECCO_version4_release1", None)
    print isLayerEnabled("TFLUX_ECCO_version4_release1", "PROD")
    print isLayerEnabled("TFLUX_ECCO_version4_release1", "SIT")
