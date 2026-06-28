#!/usr/bin/env python3
"""Run the main pipeline (bpipe). Usage: python run.py"""
from create.bpipe import app

if __name__ == "__main__":
    app.run(port=8000, debug=True)
