from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(frozen=True)
class ProjectPaths:
    project_root: Path
    config: Dict[str, Any]

    @classmethod
    def load(cls, project_root: str | Path, config_path: str | Path | None = None) -> "ProjectPaths":
        root = Path(project_root).expanduser().resolve()
        if config_path is None:
            cfg_path = root / "vrp_prod" / "config" / "paths.yaml"
            if not cfg_path.exists():
                # Allows running directly from the starter folder before copying files around.
                cfg_path = Path(__file__).resolve().parents[3] / "config" / "paths.yaml"
        else:
            cfg_path = Path(config_path).expanduser().resolve()

        with open(cfg_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return cls(project_root=root, config=config)

    def p(self, rel_path: str | Path) -> Path:
        return (self.project_root / Path(rel_path)).resolve()

    def get(self, *keys: str) -> Path:
        node: Any = self.config
        for key in keys:
            node = node[key]
        return self.p(node)
