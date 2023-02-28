from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader
import sys

module_name = "sgcollect_info"
spec = spec_from_loader(module_name, SourceFileLoader(module_name, "tools/" + module_name))
mod = module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
