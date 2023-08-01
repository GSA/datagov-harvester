from controller.source import bp, db


@bp.route('/view/<source_id>', methods=['GET'])
def harvest_view(source_id):
    print(db)
    return db[source_id]
