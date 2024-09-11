import os
import json
import traceback
from termcolor import colored, cprint
import korus.db as kdb


# TODO: rename transform_fcn to parse_fcn


class InputLogger:
    """ Class for logging input values

        Stores data in a JSON file named 'last_submit.json' in the user's home directory under $HOME/.ktam/

        Implements the methods :meth:`write` and :meth:`read` for writing and reading to the file.            
    """
    def __init__(self):
        dir_path = os.path.join(os.environ['HOME'], ".ktam")
        self.path = os.path.join(dir_path, "last_submit.json")
        
        if os.path.exists(self.path):
            self._load()

        else:
            # ensure directory exists
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            self.data = dict()
            self._dump()

    def write(self, name, value, group=None):
        """ Log a value

            Args:
                name: str
                    Parameter name
                value: 
                    The value to be logged
                group: str
                    Optional group name, used for organising the data.
                    Parameter names must be unique within each group.
        """
        if group is None:
            self.data[name] = value

        else:
            if group not in self.data:
                self.data[group] = dict()

            self.data[group][name] = value
    
        self._dump()
    
    def read(self, name, group=None):
        """ Retrieve a value

            Args:
                name: str
                    Parameter name
                group: str
                    Optional group name, used for organising the data.
        """
        self._load()
        if group is None:
            return self.data.get(name)

        else:
            g = self.data.get(group)
            if g is not None:
                return g.get(name)

        return None

    def _load(self):
        """Loads the contents of the JSON file"""
        with open(self.path, "r") as f:
            self.data = json.loads(f.read())

    def _dump(self):
        """Dumps the data to the JSON file"""
        with open(self.path, 'w') as f:
            json.dump(self.data, f)   


# maps 'keyboard input' -> 'key name'
human_readable_key = { 
    "": "<ENTER>"
}


class Option:
    """ Class for handling specific, configurable input options

        Args:
            key: str,list,tuple
                Input key(s) used to select the option 
            message: str
                Message describing actions triggered by the option
            fcn: callable
                Function carrying out the action
    """
    def __init__(self, key, message, fcn):
        self.key = key if isinstance(key, (list,tuple)) else [key]
        self.message = message
        self.fcn = fcn         

    def key_str(self):
        """Concatenates the input key(s) into a human-readable string"""
        human_key = [human_readable_key.get(k, k) for k in self.key]
        return "/".join(human_key)

    def is_selected(self, key):
        """Returns True if @key matches one of the input keys associated with this option"""
        return key in self.key


class UserInput:
    """ Interactive session for requesting user input via console.

        Args:
            name: str
                Parameter name
            message: str
                Request message presented to the user
            transform_fcn: callable
                Function applied to input provided by the user via the console.
            group: str
                Group that the parameter belongs to. Optional. 
                Parameter names must be unique within groups.
            json_fcn: callable
                Function applied to input when storing in JSON logging file.
            allowed_values: 
                Allowed input value(s).
    """
    def __init__(
        self, 
        name, 
        message,
        transform_fcn=lambda x: x,
        group=None,
        json_fcn=lambda x: x,
        allowed_values=None,
    ):
        self.group = group
        self.name = name
        self.message = message
        self.transform_fcn = transform_fcn
        self.json_fcn = json_fcn
        self.value = None
        self.options = dict()

        if allowed_values is not None and not isinstance(allowed_values, list):
            allowed_values = [allowed_values]

        self.allowed_values = allowed_values

    def add_option(self, key, message, fcn):
        """ Add a special, configurable option to the user prompt.

            key: str,list,tuple
                Input key(s) used to select the option 
            message: str
                Message describing actions triggered by the option
            fcn: callable
                Function carrying out the action
        """
        opt = Option(key, message, fcn)    
        self.options[opt.key_str()] = opt
        return opt.key_str()

    def _form_request_msg(self, include_options=False):
        """ Helper function for :meth:`request` """
        color = "green"
        s = colored(f"\n >> {self.message}", color, attrs=["bold"])
        
        if include_options:
            if len(self.options) > 0:
                s += colored("\n\n     Keyword options:", color)
            
            for opt in self.options.values():
                s += colored(f"\n      * {opt.key_str()}: {opt.message}", color)

        s += "\n"

        return s

    def request(self, logger=None):
        """ Request input from the user via the console.

            Args:
                logger: korus.app.app_util.ui.InputLogger
                    Input logger. 
        """
        if logger:
            last_inp = logger.read(self.name, group=self.group)
            opt = Option(
                "", 
                f"Reuse last input: {last_inp}", 
                lambda x: "" if last_inp in [None,""] else self.transform_fcn(last_inp)
            )
            self.options[opt.key_str()] = opt

        msg = self._form_request_msg(include_options=True)

        while True:
            try:
                inp = input(msg)

                self.selected_opt = None
                for opt in self.options.values():
                    if opt.is_selected(inp):
                        value = opt.fcn(inp)
                        self.selected_opt = opt
                        break

                if self.selected_opt and value is None:
                    msg = self._form_request_msg()
                    continue

                if not self.selected_opt:
                    value = None if inp is None else self.transform_fcn(inp)


                if self.allowed_values is not None and value not in self.allowed_values:
                    err_msg = f"Invalid input. Allowed values are: {self.allowed_values}"
                    raise ValueError(err_msg)

                break

            except Exception as e:
                cprint(traceback.format_exc(), "red")
                cprint(" ## Error processing input, please try again", "red")
                msg = self._form_request_msg(include_options=True)

        if logger:
            logger.write(self.name, self.json_fcn(value), group=self.group)

        self.value = value
        return self.value


class UserInputYesNo(UserInput):
    """ Interactive session specifically for inputting yes/no answers.

        Args:
            name: str
                Parameter name
            message: str
                Request message presented to the user
            group: str
                Group that the parameter belongs to. Optional. 
                Parameter names must be unique within groups.
    """    
    def __init__(
        self, 
        name, 
        message,
        group=None,
    ):
        def transform_fcn(x):
            if x.lower() in ["y", "yes"]:
                return True
            elif x.lower() in ["n", "no"]:
                return False
            else:
                raise ValueError

        super().__init__(
            name=name, 
            message=message,
            transform_fcn=transform_fcn,
            group=group,
            json_fcn=lambda x: "y" if x else "N",
        )


class UserInputSound(UserInput):
    """ Interactive session specifically for inputting sound-source and sound-type information.

        Args:
            name: str
                Parameter name
            message: str
                Request message presented to the user
            conn: sqlite3.Connection
                Database connection
            taxonomy_id: int
                Index of the reference taxonomy for interpreting the tags provided by the user.
            group: str
                Group that the parameter belongs to. Optional. 
                Parameter names must be unique within groups.
    """
    def __init__(self, 
        name, 
        message,
        conn,
        taxonomy_id,
        group=None,
        default=None,
    ):
        def transform_fcn(x):
            if isinstance(x, str):
                # parse user input from console
                x = x.replace("\'","\"")
                x = x.replace("(", "[")
                x = x.replace(")", "]")
                x = json.loads(x)
                if not isinstance(x, list) and len(x) > 0:
                    raise ValueError                
                if not isinstance(x[0], list):
                    x = [x]
                return x
            elif isinstance(x, list):
                # return value loaded from logger cache
                return x
            else:
                raise TypeError

        super().__init__(name, message, group=group, transform_fcn=transform_fcn)

        self.tax = kdb.get_taxonomy(conn, taxonomy_id=taxonomy_id)

        self.ui_sound_source = UserInput("sound_source", "Sound source")
        self.ui_sound_type = UserInput("sound_type", "Sound type")
        self.ui_proceed = UserInput("proceed", "Specify another source-type pair? [y/N]", transform_fcn=lambda x: x.lower() == "y")

        self.ui_sound_source.add_option(
            key=["v","view"],
            message="View allowed values",
            fcn=lambda x: self.tax.show(append_name=True)        
        )

        self.add_option(
            key=["i","iterative"],
            message="Iterative approach with look-up functionality and automatic validation",
            fcn=lambda x: self._iter_fcn()
        )

        if default:
            self.add_option(
                key=["d","default"],
                message=f"Use default value: {default}",
                fcn=lambda x: default
            )

    def _iter_fcn(self):
        """ Helper function"""
        sounds = []
        while True:

            ss = self.ui_sound_source.request()

            self.ui_sound_type.add_option(
                key=["v","view"],
                message="View allowed values",
                fcn=lambda x: self.tax.sound_types(ss).show(append_name=True)       
            )

            st = self.ui_sound_type.request()

            sounds.append((ss, st))

            proceed = self.ui_proceed.request()

            if not proceed:
                break

        return sounds


