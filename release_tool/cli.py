import click
import util
import mirna
import urllib


@click.group()
@click.option('--debug/--no-debug', '-d', default=False, envvar='ES_INDEXER_DEBUG')
@click.option('--host', '-h', default='localhost', envvar='ES_HOST')
@click.option('--port', '-p', default='9200', envvar='ES_PORT')
@click.option('--no_es', '-n', default=False, is_flag=True)
@click.option('--config', '-c', default='app.conf', envvar='ES_CONF', type=click.File())
@click.pass_context
def main(ctx, config, host, port, no_es, debug):
    # initializing ctx.obj
    ctx.obj = {}
    if no_es: host, port = (None, None)
    ctx.obj['ES_HOST'] = host
    ctx.obj['ES_PORT'] = port
    ctx.obj['DEBUG'] = debug
    if ctx.obj['DEBUG']: click.echo('Debug is on.', err=True)

    ctx.obj['APP_CTX'] = util.init_app(config, host=host, port=port)



@main.command()
@click.pass_context
def build(ctx):
    mirna.build_donor(ctx.obj['APP_CTX'])


if __name__ == '__main__':
    main()
