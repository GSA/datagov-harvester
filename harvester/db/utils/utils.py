from sqlalchemy import create_engine

def create_engine( *args ):
    return create_engine( "{}://{}:{}@{}:{}/{}".format( *args ) )
