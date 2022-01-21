# Cyware branch of Dramatiq

## TODO

* How to build
* PyPI link
* Kafka unit tests

_Upstream README below this point_

<img src="https://dramatiq.io/_static/logo.png" align="right" width="131" />

*A fast and reliable distributed task processing library for Python 3.*

<hr/>

**Changelog**: https://dramatiq.io/changelog.html <br/>
**Community**: https://groups.io/g/dramatiq-users <br/>
**Documentation**: https://dramatiq.io <br/>

<hr/>

<h3 align="center">Sponsors</h3>

<p align="center">
  <a href="https://www.sendcloud.com/?utm_source=dramatiq.io&utm_medium=Banner&utm_campaign=Sponsored%20Banner&utm_content=V1" target="_blank">
    <img width="222px" src="docs/source/_static/sendcloud-logo.png">
  </a>
</p>


## Installation

If you want to use it with [RabbitMQ]

```bash
pip install 'dramatiq[rabbitmq, watch]'
```

or if you want to use it with [Redis]

```bash
pip install 'dramatiq[redis, watch]'
```

## Quickstart

Make sure you've got [RabbitMQ] running, then create a new file called
`example.py`:

```python
import dramatiq
import requests
import sys


@dramatiq.actor
def count_words(url):
    response = requests.get(url)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")


if __name__ == "__main__":
    count_words.send(sys.argv[1])
```

In one terminal, run your workers:

    dramatiq example

In another, start enqueueing messages:

    python example.py http://example.com
    python example.py https://github.com
    python example.py https://news.ycombinator.com

Check out the [user guide] to learn more!


## License

dramatiq is licensed under the LGPL.  Please see [COPYING] and
[COPYING.LESSER] for licensing details.


[COPYING.LESSER]: https://bitbucket.org/cywarelabs/dramatiq/src/master/COPYING.LESSER
[COPYING]: https://bitbucket.org/cywarelabs/dramatiq/src/master/COPYING
[RabbitMQ]: https://www.rabbitmq.com/
[Redis]: https://redis.io
[user guide]: https://dramatiq.io/guide.html
