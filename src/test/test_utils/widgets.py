class Widgets:
    mandatory_widgets: list = ["SourceStorageAccount",
                               "DestinationStorageAccount",
                               "SourceContainer",
                               "SourceDatasetidentifier",
                               "SourceFileName",
                               "KeyColumns"]
    def __init__(self):
        self.dropdowns: dict[dict[str, Any]] = {}
        self.texts: dict[dict[str, Any]] = {}
        self.widgets: dict[dict[str, Any]] = {} 
        for widget in Widgets.mandatory_widgets:
            self.text(widget, " ", widget)
        
    def dropdown(self, name: str, default: str, values: list, label: str):
        self.dropdowns[name] = {"default": default,
                               "value": default,
                               "values": values,
                               "label" : label}
        self.widgets[name] = self.dropdowns[name].copy()

    def text(self, name: str, default: str, label: str):
        self.texts[name] = {"default": default,
                               "value": default,
                               "label" : label}
        self.widgets[name] = self.texts[name].copy()

    def get(self, key:str):
        val = self.widgets.get(key, None)
        print(val)
        return val.get("value")
    
    @staticmethod
    def remove(*args):
        pass
    
    def __str__(self):
        return f"widgets: {self.widgets}"