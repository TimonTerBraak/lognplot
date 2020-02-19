#[macro_use]
extern crate log;

mod coresight;
mod stlink;

use coresight::{MemoryAccess, MemoryAddress, Target};
use lognplot::net::TcpClient;
use stlink::{StLink, StLinkMode, StLinkResult};

fn main() {
    // simple_logger::init().unwrap();
    let matches = clap::App::new("swviewer")
        .arg(
            clap::Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            clap::Arg::with_name("elf")
                .required(true)
                .help("ELF file with debug info where to find variables."),
        )
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => log::Level::Info,
        1 => log::Level::Debug,
        2 | _ => log::Level::Trace,
    };
    let elf_filename: String = matches.value_of("elf").unwrap().to_string();

    simple_logger::init_with_level(log_level).unwrap();
    info!("Log level: {}", log_level);
    info!("rusb version: {:?}", rusb::version());

    parse_elf_file(elf_filename);

    if let Err(e) = do_magic() {
        error!("An error occurred: {:?}", e);
    }
}

fn parse_elf_file(elf_filename: String) {
    info!("Parsing {}", elf_filename);

    use object::Object;
    let data = std::fs::read(elf_filename).unwrap();
    let obj = object::File::parse(&data).unwrap();

    // Copied from: https://github.com/gimli-rs/gimli/blob/master/examples/simple.rs
    info!(
        "Parsed obj file. Has debug syms: {:?}",
        obj.has_debug_symbols()
    );

    let endian = if obj.is_little_endian() {
        gimli::RunTimeEndian::Little
    } else {
        gimli::RunTimeEndian::Big
    };

    // Define some helper closures:
    let load_section = |id: gimli::SectionId| -> Result<std::borrow::Cow<[u8]>, gimli::Error> {
        Ok(obj
            .section_data_by_name(id.name())
            .unwrap_or(std::borrow::Cow::Borrowed(&[][..])))
    };

    let load_section_sup = |_| Ok(std::borrow::Cow::Borrowed(&[][..]));

    let dwarf_cow = gimli::Dwarf::load(&load_section, &load_section_sup).unwrap();

    let borrow_section: &dyn for<'a> Fn(
        &'a std::borrow::Cow<[u8]>,
    ) -> gimli::EndianSlice<'a, gimli::RunTimeEndian> =
        &|section| gimli::EndianSlice::new(&*section, endian);

    // Create `EndianSlice`s for all of the sections.
    let dwarf = dwarf_cow.borrow(&borrow_section);

    let mut iter = dwarf.units();
    while let Some(header) = iter.next().unwrap() {
        let unit = dwarf.unit(header).unwrap();
        if let Some(name) = unit.name {
            println!("Unit: {:?}", name.to_string());
        }

        let mut entries = unit.entries();
        // entries.next_dfs().unwrap();
        while let Some((depth, entry)) = entries.next_dfs().unwrap() {
            // while let Some(entry) = entries.next_sibling().unwrap() {
            let tag = entry.tag();
            println!("  - entry: depth={}, tag={:?}", depth, entry.tag());
            if tag == gimli::DW_TAG_variable {
                println!("   -- it is a variable!");
                let mut attrs = entry.attrs();
                while let Some(attr) = attrs.next().unwrap() {
                    println!("    --- attr name: {:?}", attr.name());
                    println!("    --- attr value: {:?}", attr.value());
                }
            }
        }
    }
}

fn lsusb() -> StLinkResult<()> {
    for device_list in rusb::devices().iter() {
        info!("Device list:");
        for device in device_list.iter() {
            let desc = device.device_descriptor()?;
            info!(
                "- Device: bus={:?} vendor:product = {:04X}:{:04X}",
                device.bus_number(),
                desc.vendor_id(),
                desc.product_id()
            );
        }
    }

    Ok(())
}

fn do_magic() -> StLinkResult<()> {
    lsusb()?;
    if let Some(st_link_device) = stlink::find_st_link()? {
        info!("ST link found!");
        let sl = stlink::open_st_link(st_link_device)?;
        interact(&sl)?;
        sl.cmd_x40()?;

        if let Err(e) = interact2(&sl) {
            error!("Error: {:?}", e);
        }

        capture_trace_data(&sl)?;
    } else {
        warn!("No ST link found, please connect it?");
    }

    Ok(())
}

fn enter_proper_mode(st_link: &StLink) -> StLinkResult<()> {
    let mut mode = st_link.get_mode()?;
    info!("Mode: {:?}", mode);
    if let StLinkMode::Dfu = mode {
        st_link.leave_dfu_mode()?;
        mode = st_link.get_mode()?;
        info!("Mode: {:?}", mode);
    }

    match mode {
        StLinkMode::Dfu | StLinkMode::Mass => {
            st_link.enter_debug_mode()?;
            mode = st_link.get_mode()?;
            info!("Mode: {:?}", mode);
        }
        _ => {}
    }

    Ok(())
}

fn read_chip_id(st_link: &StLink) -> StLinkResult<()> {
    let address = 0xE004_2000; // Chip ID
    let value = st_link.read_debug32(address)?;
    info!("Chip ID is 0x{:08X}", value);
    Ok(())
}

fn interact(st_link: &StLink) -> StLinkResult<()> {
    let version = st_link.get_version()?;
    info!("ST-link Version: {:?}", version);

    enter_proper_mode(st_link)?;
    read_chip_id(st_link)?;

    Ok(())
}

fn interact2<M>(mem_access: &M) -> coresight::CoreSightResult<()>
where
    M: MemoryAccess,
{
    let mut target = Target::new(mem_access);

    target.read_debug_components()?;
    target.setup_tracing()?;
    target.start_trace_memory_address(0x2000_0004)?;
    for _a in 1..10 {
        target.poll()?;
    }

    Ok(())
}

fn capture_trace_data(st_link: &StLink) -> StLinkResult<()> {
    use scroll::{Pread, LE};
    // Send data to lognplot GUI:
    let mut client = TcpClient::new("127.0.0.1:12345").unwrap();

    // client.send_sample("bla", 1.0, 3.14);
    // client.send_sample("bla", 3.0, 3.14);
    // client.send_sample("bla", 9.0, 3.14);

    let mut timestamp: f64 = 0.0;

    let mut decoder = coresight::Decoder::new();

    // Enter trace capture:
    loop {
        // std::thread::sleep(std::time::Duration::from_millis(60));

        let trace_byte_count = st_link.get_trace_count()?;
        if trace_byte_count > 0 {
            debug!("Trace bytes: {}", trace_byte_count);
            debug!("Reading trace data.");
            let trace_data = st_link.read_trace_data(trace_byte_count)?;
            debug!("Trace data: {:?}", trace_data);

            decoder.feed(trace_data);
            while let Some(packet) = decoder.pull() {
                match packet {
                    coresight::TracePacket::TimeStamp { tc, ts } => {
                        debug!("Timestamp packet: tc={} ts={}", tc, ts);
                        let mut time_delta: f64 = ts as f64;
                        // Divide by core clock frequency to go from ticks to seconds.
                        time_delta /= 16_000_000.0;
                        timestamp += time_delta;
                        // println!("TIme: {}", timestamp);
                    }
                    coresight::TracePacket::DwtData { id, payload } => {
                        // TODO: queue?
                        debug!("Dwt: id={} payload={:?}", id, payload);
                        // timestamp += 1.0;

                        if id == 17 {
                            // TODO: grab timestamp
                            // New memory value!

                            let value: i32 = payload.pread(0).unwrap();
                            trace!("VAL={}", value);
                            client.send_sample("a", timestamp, value as f64).unwrap();
                        }
                    }
                    _ => {
                        debug!("Trace packet: {:?}", packet);
                    }
                }
            }
        }
    }
}

impl MemoryAccess for StLink {
    fn read_u32(&self, address: MemoryAddress) -> Result<u32, String> {
        self.read_debug32(address)
            .map_err(|e| format!("st-link error: {:?}", e))
    }

    fn write_u32(&self, address: MemoryAddress, value: u32) -> Result<(), String> {
        self.write_debug32(address, value)
            .map_err(|e| format!("st-link error: {:?}", e))
    }
}
