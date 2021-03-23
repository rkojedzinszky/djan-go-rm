# djan-go-rm

Generate Django-like Go models from Django models.

## Usage

Assume you are in your Django projects root, you have an application called `app1`. Then, you just invoke the generator as:

```shell
$ DJANGO_SETTINGS_MODULE=<djangoproject>.settings ../djan-go-rm/djan-go-rm.py --gomodule <go module path> app1
```

And then look for files under models/, they are ready to use.

The concept is similar than in Django, you can query objects from database through querysets. Filtering functions are generated for all fields.
