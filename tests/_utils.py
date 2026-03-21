HEADER = (
    b"CrystFEL stream format 2.3\n"
    b"Command line: indexamajig -i files.lst -o data.stream\n"
    b"----- Begin geometry file -----\n"
    b"photon_energy = /data/photon_energy\n"
    b"clen = 0.081\n"
    b"----- End geometry file -----\n"
)


def make_chunk(filename: str, event: str, indexed: bool = True) -> bytes:
    indexed_by = b"mosflm-nolatt-nocell" if indexed else b"none"
    return (
        b"----- Begin chunk -----\n"
        b"Filename: " + filename.encode() + b"\n"
        b"Event: " + event.encode() + b"\n"
        b"indexed_by: " + indexed_by + b"\n"
        b"photon_energy_eV: 9500.0\n"
        b"wavelength: 1.3042e-10 m\n"
        b"camera_length: 0.081 m\n"
        b"Peaks from peak search\n"
        b"  fs/px   ss/px (1/d)/nm^-1   Intensity  Panel\n"
        b"end_peaks\n"
        b"Reflections measured after indexing\n"
        b"   h    k    l          I   sigma(I)  peak background  fs/px  ss/px\n"
        b"end_reflections\n"
        b"----- End chunk -----\n"
    )
