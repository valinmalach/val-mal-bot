import importlib.metadata as metadata
from subprocess import call

# Upgrade packages (including pip)
packages = list({dist.metadata["Name"] for dist in metadata.distributions()})
# call("python.exe -m pip install --upgrade --user " + " ".join(packages), shell=True)
call("python.exe -m pip install --upgrade " + " ".join(packages), shell=True)
