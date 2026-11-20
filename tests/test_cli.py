from __future__ import annotations

import pytest

from ebay_agent.cli import build_parser


def test_parser_knows_every_command() -> None:
    parser = build_parser()
    assert parser.parse_args(["run", "pricing", "--approval", "deny"]).flow == "pricing"
    args = parser.parse_args(["approve", "abc", "--ids", "a,b"])
    assert args.ids == "a,b" and not args.all
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "not-a-flow"])
