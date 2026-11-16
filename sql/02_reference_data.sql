-- Reference data: the five sources (config/sources.yaml) and the NOAA HazEL descriptor
-- catalogs (workshop_1/samples/noaa_descriptor_*.json). Generated from those files; safe to run more than once.

INSERT INTO source (source_id, name, url, format, license) VALUES
    ('noaa_hazel', 'NCEI/WDS Global Historical Tsunami Database (HazEL API)', 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events', 'json', 'public use, no use constraints (dataset metadata)'),
    ('usgs_fdsn', 'USGS FDSN Event Web Service (earthquake catalog)', 'https://earthquake.usgs.gov/fdsnws/event/1/query', 'csv', 'U.S. public domain; credit requested'),
    ('usgs_realtime', 'USGS real-time GeoJSON summary feed (M4.5+, past 30 days)', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson', 'geojson', 'U.S. public domain; credit requested'),
    ('ntwc', 'National Tsunami Warning Center Atom feed (with CAP messages)', 'https://www.tsunami.gov/events/xml/PAAQAtom.xml', 'atom+cap (xml)', 'U.S. Government work, public domain'),
    ('ptwc', 'Pacific Tsunami Warning Center Atom feed (with CAP messages)', 'https://www.tsunami.gov/events/xml/PHEBAtom.xml', 'atom+cap (xml)', 'U.S. Government work, public domain')
ON CONFLICT (source_id) DO NOTHING;

INSERT INTO noaa_cause (code, description) VALUES
    (0, 'Unknown'),
    (1, 'Earthquake'),
    (2, 'Questionable Earthquake'),
    (3, 'Earthquake and Landslide'),
    (4, 'Volcano and Earthquake'),
    (5, 'Volcano, Earthquake and Landslide'),
    (6, 'Volcano'),
    (7, 'Volcano and Landslide'),
    (8, 'Landslide'),
    (9, 'Meteorological'),
    (10, 'Explosion'),
    (11, 'Astronomical Tide')
ON CONFLICT (code) DO NOTHING;

INSERT INTO noaa_event_validity (code, description) VALUES
    (-1, 'Erroneous entry'),
    (0, 'Event that only caused a seiche'),
    (1, 'Very Doubtful Tsunami'),
    (2, 'Questionable Tsunami'),
    (3, 'Probable Tsunami'),
    (4, 'Definite Tsunami')
ON CONFLICT (code) DO NOTHING;

INSERT INTO noaa_region (code, description) VALUES
    (30, 'Red Sea and Persian Gulf'),
    (40, 'Black Sea and Caspian Sea'),
    (50, 'Mediterranean Sea'),
    (60, 'Indian Ocean (including W. Australia and W. Indonesia)'),
    (70, 'Southeast Atlantic Ocean'),
    (71, 'Southwest Atlantic Ocean'),
    (72, 'Northwest Atlantic Ocean'),
    (73, 'Northeast Atlantic Ocean'),
    (74, 'Caribbean Sea'),
    (75, 'East Coast of United States and Canada'),
    (76, 'Gulf of America/Mexico'),
    (77, 'West Coast of Africa'),
    (78, 'Central Africa'),
    (80, 'Hawaii, Johnston Atoll, Midway I'),
    (81, 'E. Australia, New Zealand, South Pacific Is.'),
    (82, 'New Caledonia, New Guinea, Solomon Is., Vanuatu'),
    (83, 'E. Indonesia and Malaysia'),
    (84, 'China, North and South Korea, Philippines, Taiwan'),
    (85, 'Japan'),
    (86, 'Kamchatka and Kuril Islands'),
    (87, 'Alaska (including Aleutian Islands)'),
    (88, 'West Coast of North and Central America'),
    (89, 'West Coast of South America')
ON CONFLICT (code) DO NOTHING;

INSERT INTO noaa_warning_status (code, description) VALUES
    (-1, 'status unknown'),
    (0, 'no warning issued'),
    (1, 'PTW - Pacific-wide Tsunami Warning issued by PTWC'),
    (2, 'RTW - Regional Tsunami Warning issued by PTWC for areas having no TWS'),
    (3, 'LTW - Local Tsunami Warning issued by regional or national TWC'),
    (4, 'TIB - Tsunami Information or Attention Bulletin issued by any agency')
ON CONFLICT (code) DO NOTHING;
