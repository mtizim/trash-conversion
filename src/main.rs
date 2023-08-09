use anyhow::anyhow;
use anyhow::Error;
use anyhow::Result;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::Weekday;
use clap::Parser;
use csv::ReaderBuilder;
use icalendar::Calendar;
use icalendar::Component;
use icalendar::Event;
use icalendar::EventLike;
use std::collections::HashMap;

use std::fs::File;
use std::hash::Hash;
use std::io::Write;
use std::path::PathBuf;

fn default_output_path() -> PathBuf {
    let mut path = PathBuf::new();
    path.push("output.ics");
    path
}
/// Program for converting csv similar to PUK Piaseczno trash sheets to a calenda file
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the csv containing the data
    #[arg(short, long)]
    calendar_path: PathBuf,

    // Path to the output calendar file
    #[arg(default_value=default_output_path().into_os_string())]
    output_path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let path = &args.calendar_path;
    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_path(path)?;

    let mut csv_iter = rdr.records().enumerate();
    // Parse the csv
    let year = parse_year(&mut csv_iter)?;
    let names = parse_trash_names(&mut csv_iter)?;
    let entries = parse_trash_entries(&mut csv_iter)?;
    let conversions = parse_conversions(&mut csv_iter)?;

    let calendar = fill_calendar(entries, conversions, year, names)?;

    let mut file = File::create(args.output_path)?;
    file.write_all(format!("{}", calendar).as_bytes())?;
    file.flush()?;
    Ok(())
}

fn fill_calendar(
    entries: Vec<InputTrashEntry>,
    conversions: HashMap<SimpleDate, SimpleDate>,
    year: i32,
    names: HashMap<TrashType, String>,
) -> Result<Calendar> {
    let mut calendar = Calendar::new();
    for entry in entries {
        let mut process_date = |date: NaiveDate| {
            let simple_date = SimpleDate {
                month: date.month(),
                day: date.day(),
            };
            let converted_date = conversions.get(&simple_date).unwrap_or(&simple_date);
            let date = NaiveDate::from_ymd_opt(year, converted_date.month, converted_date.day)
                .expect("Shouldn't happen if your conversions are okay");
            let event = Event::new()
                .all_day(date)
                .summary(names.get(&entry.ty).expect("No trash type name"))
                .description(names.get(&entry.ty).expect("No trash type name"))
                .done();
            calendar.push(event);
        };
        match entry.day {
            InputTrashDate::Day(day) => process_date(
                chrono::NaiveDate::from_ymd_opt(year, entry.month_number, day)
                    .expect("Got invalid date"),
            ),
            InputTrashDate::Weekdays(weekday) => {
                (1..=5)
                    .filter_map(|n| {
                        chrono::NaiveDate::from_weekday_of_month_opt(
                            year,
                            entry.month_number,
                            weekday,
                            n,
                        )
                    })
                    .for_each(process_date);
            }
        }
    }
    Ok(calendar.done())
}

fn parse_year(
    csv_iter: &mut std::iter::Enumerate<csv::StringRecordsIter<'_, File>>,
) -> Result<i32, Error> {
    let (_, record) = csv_iter.next().expect("Empty csv");
    let record = record?;
    let year = record[1].parse::<i32>()?;
    Ok(year)
}

fn parse_trash_names(
    csv_iter: &mut std::iter::Enumerate<csv::StringRecordsIter<'_, File>>,
) -> Result<HashMap<TrashType, String>, Error> {
    let mut names: HashMap<TrashType, String> = HashMap::new();
    let (_, record) = csv_iter.next().expect("Empty csv");
    let record = record?;
    {
        let mut headerwriter = record.into_iter();
        headerwriter.next().expect("No headers");
        let mut encountered = 0;
        for entry in headerwriter {
            if entry.is_empty() {
                continue;
            }
            let ty = TrashType::from_index(encountered)?;
            names.insert(ty, entry.to_owned());
            encountered += 1;
        }
    }
    Ok(names)
}

fn parse_trash_entries(
    csv_iter: &mut std::iter::Enumerate<csv::StringRecordsIter<'_, File>>,
) -> Result<Vec<InputTrashEntry>, Error> {
    let mut entries: Vec<InputTrashEntry> = vec![];
    for (_, record) in csv_iter.by_ref() {
        let record = record?;
        let mut entries_iter = record.into_iter();
        let firstentry = entries_iter.next().expect("Empty row?");
        if firstentry.is_empty() {
            break;
        }
        let month_number = firstentry.parse::<u32>()?;

        for (i, entry) in entries_iter.enumerate() {
            let category = i / 3;
            let ty = TrashType::from_index(category)?;
            let day = entry
                .parse::<u32>()
                .map(|parsed_number| Ok::<_, Error>(InputTrashDate::Day(parsed_number)))
                .unwrap_or_else(|_| {
                    Ok(InputTrashDate::Weekdays(polish_name_to_weekday(
                        entry.to_string(),
                    )?))
                });
            let Ok(day) = day else {
                if !entry.is_empty(){
                    println!("Got unexpected data: {}",entry)
                }
                continue;
            };
            entries.push(InputTrashEntry {
                month_number,
                day,
                ty,
            })
        }
    }
    Ok(entries)
}

fn parse_conversions(
    csv_iter: &mut std::iter::Enumerate<csv::StringRecordsIter<'_, File>>,
) -> Result<HashMap<SimpleDate, SimpleDate>, Error> {
    let mut conversions: HashMap<SimpleDate, SimpleDate> = HashMap::new();
    for (_, record) in csv_iter.by_ref() {
        let record = record?;
        let mut entries_iter = record.into_iter();
        let firstentry = entries_iter.next().expect("Empty row?");
        let secondentry = entries_iter.next().expect("Weird row");
        if firstentry == "dzień" && secondentry == "za" {
            break;
        }
    }
    for (_, record) in csv_iter {
        let record = record?;
        if record.is_empty() || record[0].is_empty() {
            break;
        }
        if record[1].is_empty() {
            println!("Bad replacement formatting, second date missing");
            break;
        }
        let from: Vec<_> = record[0]
            .to_owned()
            .split('/')
            .map(|s| s.to_owned())
            .collect();
        let to: Vec<_> = record[1]
            .to_owned()
            .split('/')
            .map(|s| s.to_owned())
            .collect();

        conversions.insert(
            SimpleDate {
                month: from[1].parse()?,
                day: from[0].parse()?,
            },
            SimpleDate {
                month: to[1].parse()?,
                day: to[0].parse()?,
            },
        );
    }
    Ok(conversions)
}
struct InputTrashEntry {
    month_number: u32,
    day: InputTrashDate,
    ty: TrashType,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
struct SimpleDate {
    month: u32,
    day: u32,
}

enum InputTrashDate {
    Day(u32),
    Weekdays(Weekday),
}

fn polish_name_to_weekday(name: String) -> Result<Weekday> {
    match name.as_str() {
        "poniedziałek" | "pon" | "poniedzialek" => Ok(Weekday::Mon),
        "wtorek" | "wto" => Ok(Weekday::Tue),
        "środa" | "śro" | "sro" => Ok(Weekday::Wed),
        "czwartek" | "cz" | "czw" => Ok(Weekday::Thu),
        "piątek" | "pią" | "pia" | "pt" => Ok(Weekday::Fri),
        "sobota" | "sob" => Ok(Weekday::Sat),
        "niedziela" | "niedz" | "nie" => Ok(Weekday::Sun),
        _ => Err(anyhow!(format!("Couldn't parse weekday {}", name))),
    }
}

#[derive(PartialEq, Eq, Hash)]
enum TrashType {
    Mixed,
    Metal,
    Paper,
    Glass,
    Bio,
    Big,
    ChristmasTree,
}
impl TrashType {
    fn from_index(num: usize) -> Result<Self> {
        match num {
            0 => Ok(TrashType::Mixed),
            1 => Ok(TrashType::Metal),
            2 => Ok(TrashType::Paper),
            3 => Ok(TrashType::Glass),
            4 => Ok(TrashType::Bio),
            5 => Ok(TrashType::Big),
            6 => Ok(TrashType::ChristmasTree),
            _ => Err(anyhow!("Shouldn't happen")),
        }
    }
}
