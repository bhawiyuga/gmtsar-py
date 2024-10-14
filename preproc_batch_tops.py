import sys
import os
import subprocess
import glob
import datetime

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode('utf-8'), error.decode('utf-8')

def print_usage():
    print("""
Usage: preproc_batch_tops.py data.in dem.grd mode

  preprocess and align a set of tops images in data.in, precise orbits required

  format of data.in:
    image_name:orbit_name

  example of data.in
    s1a-iw1-slc-vv-20150626...001:s1a-iw1-slc-vv-20150626...001:s1a-iw1-slc-vv-20150626...001:S1A_OPER_AUX_POEORB_V20150601_20150603.EOF
    s1a-iw1-slc-vv-20150715...001:s1a-iw1-slc-vv-20150715...001:s1a-iw1-slc-vv-20150715...001:S1A_OPER_AUX_POEORB_V20150625_20150627.EOF

  outputs:
    baseline.ps align_table.ra (contains info for precise geomatric alignment)
    *.PRM *.LED *.SLC(mode 2)

  Note:
    The names must be in time order in each line to be stitched together

Reference: Xu, X., Sandwell, D.T., Tymofyeyeva, E., González-Ortega, A. and Tong, X., 
    2017. Tectonic and Anthropogenic Deformation at the Cerro Prieto Geothermal 
    Step-Over Revealed by Sentinel-1A InSAR. IEEE Transactions on Geoscience and 
    Remote Sensing.
""")
    sys.exit(1)

if len(sys.argv) != 4:
    print_usage()

# OK check the parameter
data_in = sys.argv[1]
dem_grd = sys.argv[2]
mode = int(sys.argv[3])
sl = 1

# OK Sample the dem
if mode == 2:
    run_command(f"gmt grdfilter {dem_grd} -D2 -Fg2 -I12s -Gflt.grd")
    run_command("gmt grd2xyz --FORMAT_FLOAT_OUT=%lf flt.grd -s > topo.llt")

# OK First line is the super-master, all images aligned to it
with open(data_in, 'r') as f:
    first_line = f.readline().strip()

master = f"S1_{first_line.split(':')[0][15:23]}_{first_line.split(':')[0][24:30]}_F{first_line.split(':')[0][6]}"
mmaster = f"S1_{first_line.split(':')[0][15:23]}_ALL_F{first_line.split(':')[0][6]}"

# OK Clean up
for file in glob.glob("*.PRM*") + glob.glob("*.SLC") + glob.glob("*.LED") + glob.glob("tmp*"):
    os.remove(file)

if mode == 1:
    if os.path.exists("baseline_table.dat"):
        os.remove("baseline_table.dat")

# OK Loop over all the acquisitions
with open(data_in, 'r') as f:
    for line in f:
        line = line.strip()
        # OK record the first one as the stem_master
        stem_master = f"S1_{line.split(':')[0][15:23]}_{line.split(':')[0][24:30]}_F{line.split(':')[0][6]}"
        m_stem_master = f"S1_{line.split(':')[0][15:23]}_ALL_F{line.split(':')[0][6]}"

        if mode == 1:
            # OK Mode 1: Generate baseline plots
            image = line.split(':')[0]
            orbit = line.split(':')[-1]

            # OK Generate PRMs and LEDs
            run_command(f"make_s1a_tops {image}.xml {image}.tiff {m_stem_master} 0")
            run_command(f"ext_orb_s1a {m_stem_master}.PRM {orbit} {m_stem_master}")

            # OK Get the height and baseline info
            run_command(f"cp {m_stem_master}.PRM junk1")
            run_command("calc_dop_orb junk1 junk2 0 0")
            run_command(f"cat junk1 junk2 > {m_stem_master}.PRM")

            run_command(f"baseline_table.csh {mmaster}.PRM {m_stem_master}.PRM >> baseline_table.dat")
            run_command(f"baseline_table.csh {mmaster}.PRM {m_stem_master}.PRM GMT >> table.gmt")

            # OK Clean up
            os.remove("junk1")
            os.remove("junk2")

        elif mode == 2:
            # Mode 2: Stitch and align all the images
            line = line.strip()
            files = line.split(':')[:-1]
            orbit = line.split(':')[-1]

            # OK Write files to be stitched to a temporary file
            with open('tmp.filelist', 'w') as tmp_file:
                for file in files:
                    tmp_file.write(f"{file}\n")

            if os.path.exists('tmp.stitchlist'):
                os.remove('tmp.stitchlist')
            tmp_da = 0

            # OK Remove any existing par files
            for file in glob.glob('*par*'):
                os.remove(file)

            # Process each file in the line
            for file in files:
                stem = f"S1_{file[15:23]}_{file[24:30]}_F{file[6]}"

                if sl == 1:
                    # OK Processing super-master image
                    run_command(f"make_s1a_tops {file}.xml {file}.tiff {stem} 1")
                    run_command(f"ext_orb_s1a {stem}.PRM {orbit} {stem}")
                    with open('tmp.stitchlist', 'a') as stitch_file:
                        stitch_file.write(f"{stem}\n")
                else:
                    # OK Processing slave images
                    run_command(f"make_s1a_tops {file}.xml {file}.tiff {stem} 0")
                    run_command(f"ext_orb_s1a {stem}.PRM {orbit} {stem}")

                    # OK Compute time difference and shift
                    t1 = float(run_command(f"grep clock_start {stem_master}.PRM | grep -v SC_clock_start")[0].split()[2])
                    t2 = float(run_command(f"grep clock_start {stem}.PRM | grep -v SC_clock_start")[0].split()[2])
                    prf = float(run_command(f"grep PRF {stem_master}.PRM")[0].split()[2])
                    nl = int((t2 - t1) * prf * 86400.0 + 0.2)

                    print(f"Shifting the master PRM by {nl} lines...")
                    run_command(f"cp {master}.PRM tmp.PRM")

                    # OK Update PRM file with shifted times
                    for param in ['clock_start', 'clock_stop', 'SC_clock_start', 'SC_clock_stop']:
                        old_time = float(run_command(f"grep {param} tmp.PRM")[0].split()[2])
                        new_time = old_time + nl / prf / 86400.0
                        run_command(f"update_PRM tmp.PRM {param} {new_time:.12f}")

                    # OK Compute image offset
                    if tmp_da == 0:
                        # ... (compute tmp_da)
                        run_command(f"cp tmp.PRM junk1.PRM")
                        run_command(f"cp {stem}.PRM junk2.PRM")
                        run_command(f"calc_dop_orb junk1.PRM junk {earth_radius} 0")
                        run_command("cat junk >> junk1.PRM")
                        run_command(f"calc_dop_orb junk2.PRM junk {earth_radius} 0")
                        run_command("cat junk >> junk2.PRM")
                        
                        lontie = float(run_command("SAT_baseline junk1.PRM junk2.PRM | grep lon_tie_point")[0].split()[2])
                        lattie = float(run_command("SAT_baseline junk1.PRM junk2.PRM | grep lat_tie_point")[0].split()[2])
                        
                        tmp_am = float(run_command(f"echo {lontie} {lattie} 0 | SAT_llt2rat tmp.PRM 1")[0].split()[1])
                        tmp_as = float(run_command(f"echo {lontie} {lattie} 0 | SAT_llt2rat {stem}.PRM 1")[0].split()[1])
                        tmp_da = int(tmp_as - tmp_am)
                        
                        os.remove("junk1.PRM")
                        os.remove("junk2.PRM")
                        os.remove("junk")

                    # OK Handle large offsets
                    if -1000 < tmp_da < 1000:
                        run_command(f"cp tmp.PRM junk1")
                        run_command(f"calc_dop_orb junk1 junk2 {earth_radius} 0")
                        run_command("cat junk1 junk2 > tmp.PRM")
                        run_command(f"cp {stem}.PRM junk1")
                        run_command(f"calc_dop_orb junk1 junk2 {earth_radius} 0")
                        run_command(f"cat junk1 junk2 > {stem}.PRM")
                        os.remove("junk1")
                        os.remove("junk2")

                        run_command("SAT_llt2rat tmp.PRM 1 < topo.llt > tmpm.dat")
                        run_command(f"SAT_llt2rat {stem}.PRM 1 < topo.llt > tmp1.dat")
                    else:
                        print(f"Modifying master PRM by {tmp_da} lines...")
                        prf = float(run_command("grep PRF tmp.PRM")[0].split()[2])
                        
                        for param in ['clock_start', 'clock_stop', 'SC_clock_start', 'SC_clock_stop']:
                            old_time = float(run_command(f"grep {param} tmp.PRM | grep -v SC_{param}")[0].split()[2])
                            new_time = old_time - tmp_da / prf / 86400.0
                            run_command(f"update_PRM tmp.PRM {param} {new_time:.12f}")
                        
                        run_command("cp tmp.PRM junk1")
                        run_command(f"calc_dop_orb junk1 junk2 {earth_radius} 0")
                        run_command("cat junk1 junk2 > tmp.PRM")
                        run_command(f"cp {stem}.PRM junk1")
                        run_command(f"calc_dop_orb junk1 junk2 {earth_radius} 0")
                        run_command(f"cat junk1 junk2 > {stem}.PRM")
                        os.remove("junk1")
                        os.remove("junk2")
                        
                        run_command("SAT_llt2rat tmp.PRM 1 < topo.llt > tmpm.dat")
                        run_command(f"SAT_llt2rat {stem}.PRM 1 < topo.llt > tmp1.dat")

                    # OK get r, dr, a, da, SNR table to be used by fitoffset.csh
                    run_command("paste tmpm.dat tmp1.dat | awk '{printf(\"%.6f %.6f %.6f %.6f %d\\n\", $1, $6 - $1, $2, $7 - $2, 100)}' > tmp.dat")
                    rmax = int(run_command(f"grep num_rng_bins {stem}.PRM")[0].split()[2])
                    amax = int(run_command(f"grep num_lines {stem}.PRM")[0].split()[2])
                    run_command(f"awk '{{if($1 > 0 && $1 < {rmax} && $3 > 0 && $3 < {amax}) print $0 }}' < tmp.dat > offset.dat")

                    # OK Prepare offset data
                    # ... (prepare offset.dat and par_tmp.dat)

                    if tmp_da > -1000 and tmp_da < 1000:
                        run_command(f"awk '{{printf(\"%.6f %.6f %.6f %.6f %d\\n\", $1, $2, $3 + {nl}, $4, $5)}}' < offset.dat >> par_tmp.dat")
                    else:
                        run_command(f"awk '{{printf(\"%.6f %.6f %.6f %.6f %d\\n\", $1, $2, $3 + {nl} - {tmp_da}, $4 + {tmp_da}, $5)}}' < offset.dat >> par_tmp.dat")


                    # OK Prepare shift look-up tables
                    # ... (prepare r.grd and a.grd)
                    run_command("awk '{ printf(\"%.6f %.6f %.6f \\n\",$1,$3,$2) }' < offset.dat > r.xyz")
                    run_command("awk '{ printf(\"%.6f %.6f %.6f \\n\",$1,$3,$4) }' < offset.dat > a.xyz")
                    run_command(f"gmt blockmedian r.xyz -R0/{rmax}/0/{amax} -I8/4 -r -bo3d > rtmp.xyz")
                    run_command(f"gmt blockmedian a.xyz -R0/{rmax}/0/{amax} -I8/4 -r -bo3d > atmp.xyz")
                    run_command(f"gmt surface rtmp.xyz -bi3d -R0/{rmax}/0/{amax} -I8/4 -Grtmp.grd -T0.5 -N1000 -r")
                    run_command(f"gmt surface atmp.xyz -bi3d -R0/{rmax}/0/{amax} -I8/4 -Gatmp.grd -T0.5 -N1000 -r")
                    run_command("gmt grdmath rtmp.grd FLIPUD = r.grd")
                    run_command("gmt grdmath atmp.grd FLIPUD = a.grd")

                    # OK Generate image with point-by-point shifts
                    run_command(f"make_s1a_tops {file}.xml {file}.tiff {stem} 1 r.grd a.grd")

                    # OK Update shift parameters
                    run_command(f"fitoffset.csh 3 3 offset.dat >> {stem}.PRM")

                    with open('tmp.stitchlist', 'a') as stitch_file:
                        stitch_file.write(f"{stem}\n")

            # OK Stitch images together
            nf = len(open('tmp.stitchlist').readlines())
            stem = f"S1_{files[0][15:23]}_ALL_F{files[0][6]}"

            if nf > 1:
                run_command(f"stitch_tops tmp.stitchlist {stem}")
            else:
                # OK Handle single file case
                tmp_stem = open('tmp.stitchlist', 'r').read().strip()
                run_command(f"cp {tmp_stem}.PRM {stem}.PRM")
                run_command(f"cp {tmp_stem}.LED {stem}.LED")
                os.rename(f"{tmp_stem}.SLC", f"{stem}.SLC")

                run_command(f"update_PRM {stem}.PRM input_file {stem}.raw")
                run_command(f"update_PRM {stem}.PRM SLC_file {stem}.SLC")
                run_command(f"update_PRM {stem}.PRM led_file {stem}.LED")

            run_command(f"ext_orb_s1a {stem}.PRM {orbit} {stem}")

            # OK Handle non-super-master images
            if sl != 1:
                # ... (resample and update PRM)
                run_command(f"cp {stem}.PRM {stem}.PRM0")
                if tmp_da > -1000 and tmp_da < 1000:
                    run_command(f"update_PRM {stem}.PRM ashift 0")
                else:
                    run_command(f"update_PRM {stem}.PRM ashift {tmp_da}")
                    print(f"Restoring {tmp_da} lines shift to the image...")
                run_command(f"update_PRM {stem}.PRM rshift 0")
                
                run_command(f"resamp {mmaster}.PRM {stem}.PRM {stem}.PRMresamp {stem}.SLCresamp 1")
                os.rename(f"{stem}.PRMresamp", f"{stem}.PRM")
                os.rename(f"{stem}.SLCresamp", f"{stem}.SLC")
                
                run_command(f"fitoffset.csh 3 3 par_tmp.dat >> {stem}.PRM")

            # ok Final PRM updates
            run_command(f"cp {stem}.PRM junk1")
            if sl == 1:
                run_command("calc_dop_orb junk1 junk2 0 0")
                earth_radius = float(run_command("grep earth_radius junk2")[0].split()[2])
            else:
                run_command(f"calc_dop_orb junk1 junk2 {earth_radius} 0")
            run_command(f"cat junk1 junk2 > {stem}.PRM")
            os.remove("junk1")
            os.remove("junk2")
            sl = 2
            

# OK For mode 1, plot the time-baseline figure
if mode == 1:
    run_command("awk '{print 2014+$1/365.25,$2,$7}' < table.gmt > text")
    region = run_command("gmt gmtinfo text -C")[0].split()
    region = [float(region[0])-0.5, float(region[1])+0.5, float(region[2])-50, float(region[3])+50]

    run_command(f"gmt pstext text -JX8.8i/6.8i -R{region[0]}/{region[1]}/{region[2]}/{region[3]} -D0.2/0.2 -X1.5i -Y1i -K -N -F+f8,Helvetica+j5 > baseline.ps")
    run_command("awk '{print $1,$2}' < text > text2")
    run_command(f"gmt psxy text2 -Sp0.2c -G0 -R -JX -Ba0.5:\"year\":/a50g00f25:\"baseline (m)\":WSen -O >> baseline.ps")

    # Clean up
    os.remove("text")
    os.remove("text2")
    os.remove("table.gmt")

# Clean up
if mode == 2:
    for file in glob.glob("tmp*") + ["topo.llt", "flt.grd"] + glob.glob("atmp*") + glob.glob("rtmp*"):
        os.remove(file)