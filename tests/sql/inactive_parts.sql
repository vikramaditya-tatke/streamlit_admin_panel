SELECT count()
FROM system.parts
WHERE database = {database:String} AND table = 'events_alpha' AND NOT active
