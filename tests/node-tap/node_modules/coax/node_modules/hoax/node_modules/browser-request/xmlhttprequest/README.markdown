Goals
=====

1. Deliver unobtrusive standard-compliant (W3C) cross-browser implementation of the
   [XMLHttpRequest 1.0 object][1]
2. Fix ALL browsers quirks observed in their native XMLHttpRequest object implementations
3. Enable transparent logging of XMLHttpRequest object activity

How To Use
==========

```html
<head>
    ...
        <script type="text/javascript" src="XMLHttpRequest.js"></script>
    ...
</head>
```

Known issues
============

implementation doesn't throw errors on accessing `status` and `statusText`
properties in an inappropriate moment of time

License
=======
See the `LICENSE` file distributed with the source code.

Links to online resources
=========================

1. [XMLHttpRequest object implementation explained][2]

[1]: http://www.w3.org/TR/XMLHttpRequest
[2]: http://www.ilinsky.com/articles/XMLHttpRequest