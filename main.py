"""Compatibility entry point: the existing Windows startup target still works."""

if __name__ == "__main__":
    from fgpbot.cli import main

    raise SystemExit(main())
